import pandas as pd
import luigi
import recordlinkage
from pathlib import Path

from journals.config import PROCESSING_DATA_DIR, INPUT_DATA_DIR, OUTPUT_DATA_DIR, logger
from journals.tasks.preprocess import PreprocessChildrenTask, PreprocessParentsTask
from journals.compare import CompareSet, process_pool_compare
from journals.tasks.abbrv import AbbreviationsTask
from journals.tasks.title import TitleComparisonTask



class PredictParentRecord(luigi.Task):

    child_file_path = luigi.PathParameter()
    parent_file_path = luigi.PathParameter(default=INPUT_DATA_DIR / 'parents_250224.xlsx')

    def requires(self):

        return TitleComparisonTask(
            child_task = PreprocessChildrenTask(input_file_path=self.child_file_path),
            parent_task = PreprocessParentsTask(input_file_path=self.parent_file_path),
        )

    def run(self):

        predicted = []
        title_comparison_task = self.requires()
        child_task = title_comparison_task.child_task
        parent_task = title_comparison_task.parent_task

        child_df = pd.read_pickle(child_task.output().path)
        parent_df = pd.read_pickle(parent_task.output().path)   

        title_features = pd.read_pickle(title_comparison_task.output().path)  


        title_features = self._filter_title_features_by_match(title_features)     


        logger.info(f'Best match title features: {len(title_features)}') 

        logger.info(f'Comparing datasets by metadata columns') 
        features = self._compare_metadata(title_features.index, child_df, parent_df)        

        features["pos"] = (features == 1).sum(axis=1)
        features["neg"] = (features == -1).sum(axis=1)    

        features.to_pickle(PROCESSING_DATA_DIR / 'features.pkl')

        for mms_id, group in features.groupby(level='mms_id'):
            best_matches = group[(group["pos"] == group["pos"].max()) & (group["neg"] == group["neg"].min())]

            if best_matches.empty or len(best_matches) > 1:
                continue 

            pred_barcode = best_matches.index.get_level_values('barcode')[0]         
            predicted.append((mms_id, pred_barcode))

        df = pd.DataFrame(predicted, columns=['mms_id', 'barcode'])
        percent_predicted = len(predicted) / len(child_df) * 100

        logger.critical(f'Output predictions {len(predicted)}/{len(child_df)} ({percent_predicted})') 

        df.to_csv(self.output().path, index=False)   

    def output(self):
        title_comparison_task = self.requires()
        child_task_filename = Path(title_comparison_task.child_task.output().path).stem
        parent_task_filename = Path(title_comparison_task.parent_task.output().path).stem        
        output_file_name = f"{child_task_filename}-{parent_task_filename}-results.csv"
        return luigi.LocalTarget(OUTPUT_DATA_DIR / output_file_name)


    def _filter_title_features_by_match(self, features: pd.DataFrame):
        matches = []

        # Group the features by mms_id
        grouped_features = features.groupby(level=0)
        for idx, group in grouped_features:

            # For each mms_id group, keep the rows that match the maximum
            # jaro value, or the maximum partial ration with a high jaro
            # max (within 0.2 of the jaro max) 
            jaro_max = group.title_jaro.max()
            partial_ratio_max = group.title_partial_ratio.max()

            # Get the best matches for each mms_id
            best_matches = group[(group.title_jaro == jaro_max) | ((group.title_jaro >= jaro_max - 0.2) & (group.title_partial_ratio == partial_ratio_max))]
            matches.append(best_matches)

        return pd.concat(matches)             

    def _compare_metadata(self, candidate_links, child_df, parent_df):

        abbrv_fields = AbbreviationsTask().fields()

        matching_cols = set(child_df.columns).intersection(set(parent_df.columns))
        cols_to_compare = matching_cols.intersection(abbrv_fields)
        cols_to_compare.add('combined_years')
        cols_to_compare.discard('year')

        compare = recordlinkage.Compare() 
        for col in cols_to_compare:
            compare.add(CompareSet(col, col, label=col)) 

        features = process_pool_compare(candidate_links, compare, child_df, parent_df)

        features.index.names = ['child', 'parent']
        features = features.join(child_df[["mms_id"]], on="child")
        features = features.join(parent_df[["barcode"]], on="parent")
        features = features.set_index(['mms_id', 'barcode'])

        return features          



if __name__ == "__main__":
    luigi.build([PredictParentRecord(
        child_file_path=INPUT_DATA_DIR / 'children_to_link_250224.xlsx'
    )], local_scheduler=True)    

