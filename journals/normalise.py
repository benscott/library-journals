import pandas as pd
import re
import string

from journals.tasks.abbrv import AbbreviationsTask
from journals.config import PROCESSING_DATA_DIR, logger

class DataNormaliser(object):

    year_cols = set(['date_year', 'year'])

    re_roman = re.compile(r'\b[MCDXLVI]+\b', re.IGNORECASE)

    # Define Roman numeral mapping
    roman_to_int_map = {
        'I': 1, 'V': 5, 'X': 10, 'L': 50,
        'C': 100, 'D': 500, 'M': 1000
    }    

    def __init__(self):
        self.abbrv_data = AbbreviationsTask().read()
        self.abbrv_fields = AbbreviationsTask().fields()

    def _get_columns(self, df):
        return set(df.columns).intersection(self.abbrv_fields | self.year_cols)    

    def _normalise_value(self, value) -> set:
        """
        Normalise values and convert to set
        """
        result = set()

        if pd.isna(value):
            return result

        # Split by commas
        parts = str(value).split(',')
        for part in parts:
            # Check for range (e.g., "10-15")
            if '-' in part:            
                try:
                    start, end = map(self._to_int, part.split('-'))
                    result.update(range(start, end + 1))
                except ValueError:
                    logger.debug(f'Could not parse value {value}')
                except TypeError:
                    # Cannot parse these into range
                    # So add as individual items
                    result.update([start, end])
            else:
                # Single number
                if i:=self._to_int(part):
                    result.add(i)
                
        return result  

    def _to_int(self, text):

        if self.re_roman.match(text):
            text = self._roman_to_int(text.strip())
        try:
            return int(text)
        except ValueError:
            return text      

    def _roman_to_int(self, roman):
        """
        Convert roman numeral to int
        """
        total = 0
        prev_value = 0
        for char in reversed(roman):
            value = self.roman_to_int_map[char.upper()]
            # If the current value is less than the previous, subtract it
            if value < prev_value:
                total -= value
            else:
                total += value
            prev_value = value
        return total   

    @staticmethod
    def _is_valid_year(y):
        try:
            y = abs(y)
        except TypeError:
            return False
        else:
            return 1000 <= abs(y) <= 2025

    def _valid_years_filter(self, year_set):
        return {y for y in year_set if self._is_valid_year(y)}  

    def _map_abbrv_columns(self, df):

        en_mappings = {}
        for k, v in self.abbrv_data.items():
            if en:=v.get('en'):
                en_mappings[k] = en
                abbrvs = v.get('abbrv', [])
                for abbrv in abbrvs:
                    en_mappings[abbrv] = en     
        
        for col in set(df.columns):   
            if en:=en_mappings.get(col):
                # Does the english name, already exist as a column
                if en in df.columns:
                    df[en] = df.apply(lambda x: x[col] | x[en], axis=1)
                    df = df.drop(columns=[col])
                else:
                    df = df.rename(columns={col: en})
                    
        return df    
    
    @staticmethod
    def _normalise_title(title):
        title = title.lower()
        title = title.strip(string.whitespace + string.punctuation)
        return title
                
    def normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = self._get_columns(df)

        for column in columns:
            df[column] = df[column].apply(self._normalise_value)

        df = self._map_abbrv_columns(df)

        df['date_year'] = df['date_year'].apply(self._valid_years_filter)    
        df['year'] = df['year'].apply(self._valid_years_filter)    
        df['combined_years'] = df['date_year'].combine(df['year'], lambda x, y: x.union(y))

        df['title'] = df['title'].apply(self._normalise_title)
        df = df.applymap(lambda x: None if isinstance(x, set) and not x else x)        

        return df




if __name__ == "__main__":

    df = pd.read_csv(PROCESSING_DATA_DIR / 'preprocessed/children_with_links_250224.csv') 

    normaliser = DataNormaliser()
    df = normaliser.normalise(df)
    print(df)