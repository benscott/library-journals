import pandas as pd
from pathlib import Path
import spacy
from spacy.matcher import Matcher
import yaml
from spacy.tokens import Doc, DocBin, Span
from spacy.util import filter_spans
from sutime import SUTime
import string
import re
from datetime import datetime, timedelta
from tqdm import tqdm
import luigi

from journals.config import DATA_DIR, INPUT_DATA_DIR, PROCESSING_DATA_DIR
from journals.utils import clean_text
from journals.parser import TextParser, Params773Parser, DateParser
from journals.normalise import DataNormaliser

class PreprocessTask(luigi.Task):

    input_file_path = luigi.PathParameter()
    limit = luigi.IntParameter(default=None)

    text_parser = TextParser()

    normaliser = DataNormaliser()

    def output(self):
        output_file_name = f'{self.input_file_path.stem}.preprocessed.pkl'
        if self.limit:
            output_file_name = f'{self.limit}-{output_file_name}'
        return luigi.LocalTarget(PROCESSING_DATA_DIR / output_file_name)
    
    def read_input(self):
        params = {}

        if self.limit:
            params['nrows'] = self.limit

        return pd.read_excel(self.input_file_path, **params)           

class PreprocessParentsTask(PreprocessTask):

    input_file_path = luigi.PathParameter()

    def run(self):

        df = self.read_input()
        df = df[df.Description.notnull()]

        records = []

        for index, row in tqdm(df.iterrows(), total=len(df)):
            data = self.text_parser.parse(row.Description)
            data['mms_id'] = row['MMS Id']
            data['title'] = row['Title']
            data['description'] = row['Description']
            data['barcode'] = row['Barcode']
            records.append(data)

        df = pd.DataFrame(records)
        df['mms_id'] = df['mms_id'].astype(str)
        df['barcode'] = df['barcode'].astype(str)

        df = self.normaliser.normalise(df)
        df.to_pickle(self.output().path)


class PreprocessChildrenTask(PreprocessTask):

    param_parser = Params773Parser()
    date_parser = DateParser()

    def run(self):

        children = []

        df = self.read_input()
        df = df[df.Series.notnull()]
        df = self.param_parser.parse(df)

        params_cols = [c for c in df.columns if c.startswith('param_')]

        for i, row in tqdm(df.iterrows(), total=len(df)):

            series_title = row['Series']
            series_statement = row['Series Statement']

            series_title_params = self.text_parser.parse(series_title)  

            data = {
                'title': series_title_params.get('title')
            }

            # List of params, to be merged into one object
            all_params = []

            # We will take the first named parameter, and discard later ones
            # So metadata will take precedence over title, and then series statement
            if pd.notna(row.param_metadata):
                # Parse any metadata parameters
                metadata = self.text_parser.parse(row.param_metadata)
                all_params.append(metadata)

            all_params.append(series_title_params)

            if pd.notna(series_statement):

                statement_data = self.text_parser.parse(series_statement)
                all_params.append(statement_data)

            data = self._merge_data(data, all_params)
            data |= {p: row[p] for p in params_cols if row[p]}
            
            # Title is already added in parse text
            data['title_verbatim'] = series_title
            data['statement_verbatim'] = series_statement
            data['mms_id'] = row['MMS Id']

            if pd.notna(row['Publication Date']) and row['Publication Date'] != 0:
                try:
                    if date := self.date_parser.parse(row['Publication Date']):
                        for k, v in date.items():
                            data[f'date_{k}'] = v  
                except TypeError:
                    print('Date error: ', row['Publication Date'])
                    
            children.append(data)

        child_df = pd.DataFrame(children)
        child_df['mms_id'] = child_df['mms_id'].astype(str)
        
        child_df.rename(columns={"title": "orig_title"}, inplace=True)

        child_df['title'] = child_df.apply(lambda x: x.param_title if pd.notnull(x.param_title) else x.orig_title, axis=1)

        child_df = self.normaliser.normalise(child_df)
        child_df.to_pickle(self.output().path)

        # child_df.to_csv(self.output().path)

    @staticmethod
    def _merge_data(data, params):

        for param_dict in params:    
            for k, v in param_dict.items():
                if k not in data:
                    data[k] = v

        return data


if __name__ == "__main__":
    # luigi.build([PreprocessChildrenTask(
    #     input_file_path=INPUT_DATA_DIR / 'children_with_links_250224.xlsx',
    #     # limit=100
    # )], local_scheduler=True)

    luigi.build([PreprocessParentsTask(
        input_file_path=INPUT_DATA_DIR / 'parents_250224.xlsx',
        # limit=100
    )], local_scheduler=True)    


