from sutime import SUTime
import re
import pandas as pd
from datetime import datetime, timedelta

from journals.config import JAR_DIR
from journals.utils import clean_text
from journals.abbrv import AbbreviationMatcher

class DateParser(object):

    def __init__(self):
        self.sutime = SUTime(jars=JAR_DIR, mark_time_ranges=True, include_range=True)

    def parse(self, text):
        su_date = self.sutime.parse(text)
        if not su_date: return

        if len(su_date) > 1:
            date = {}
            for d in su_date:            
                parsed = self._parse_sutime(d)
                if not parsed: continue
                for k, v in parsed.items():
                    if k in date:
                        if k == 'year':
                            date[k].update(v)
                        else:
                            date[k] += f'; {v}'
                    else:
                        date[k] = v
        else:
            date = self._parse_sutime(su_date[-1])

        if date:
                        
            if len(date['year']) == 1:
                date['year'] = next(iter(date['year']))
            else:
                # Dataframe doesnt handly year so well
                min_y = min(date['year'])
                max_y = max(date['year'])
                date['year'] = f'{min_y}-{max_y}'
        
        return date      

    def _parse_sutime(self, d):

        if not self._has_year(d['text']):
            return None

        date = {
            'verbatim': d['text'],
        }
        
        if d['type'] == 'DURATION':    
            date['year'] = self._parse_years(d['text'])
            
        elif d['type'] == 'DATE':  
            
            if 'timex-value' in d:
                dt = None
                timex = d['timex-value']

                if timex.count('-') == 2:
                    try:
                        dt = datetime.strptime(timex, "%Y-%m-%d")
                    except ValueError:
                        pass
                elif timex.count('-') == 1:
                    try:
                        dt = datetime.strptime(timex, "%Y-%m")
                    except ValueError:
                        pass

                if dt:
                    date['year'] = set([dt.year])
                    date['date'] = timex                            
                else:
                    date['year'] = self._parse_years(timex)
            else:
                date['year'] = self._parse_years(d['text'])

        else:
            date['year'] = self._parse_years(d['text'])

        return date      

    @staticmethod
    def _has_year(text):
        return re.search(r"\b\d{2}\b|\b\d{4}\b", text)
    
    @staticmethod
    def _parse_years(text):
        years = re.findall(r"\b\d{4}\b", text)
        years = {int(y) for y in years}
        return years    


class TextParser(object):

    """
    Title text and descriptions parser
    """

    def __init__(self):
        self.matcher = AbbreviationMatcher()
        self.date_parser = DateParser()

    def parse(self, text):

        data = {}
        text = clean_text(text)

        matches, title = self.matcher.parse(text)
        date = self.date_parser.parse(text)

        data['title'] = title

        # If we have Dec in the date, ensure it hasn't also be identified by the NER 
        if date:

            if 'dec' in date['verbatim'].lower():
                matches.pop("decade", None)

            if date_pos := text.find(date['verbatim']):
                end = date_pos + len(date['verbatim'])
                try:
                    next_char = text[end]
                except IndexError:
                    pass
                else:
                    if next_char == '-':
                        date = None

        if date:
            for k, v in date.items():
                data[f'date_{k}'] = v
        
        for k, m in matches.items():

            # Make sure date hasn't been included
            if date and k != 'title' and date['verbatim'] in m:
                m = m.replace(date['verbatim'], '')

            m = m.strip(' ,')
            # If we still have a match after removing date etc.,
            if m:
                data[k] = m

        return data
    

class Params773Parser(object):

    field_name = '773 - Local Param 06'

    param_mappings = {
        'param_title': set(['t', 'a']),
        'param_subtitle': set(['b']),
        'param_metadata': set(['g', 'v']),
        'param_wid': set(['w']),
    }
    
    new_columns = list(param_mappings.keys()) + ['param_parent_barcode']

    def _get_param_mapping(self, k):
        for param, ks in self.param_mappings.items():
            if k in ks: 
                return param
            
    def _params_to_columns(self, text):
        data = self._parse_text(text)
        return pd.Series([data.get(k) for k in self.new_columns])            
    
    def _parse_text(self, text):
        
        # Regular expression to match ‡ followed by a letter and its corresponding text
        pattern = r"‡([a-z])\s(.*?)(?=; ‡|$)"
        
        # Find all matches
        matches = re.findall(pattern, text)  
        data = {}
    
        for k, v in matches:
             
            if param := self._get_param_mapping(k):                

                if param == 'param_metadata':
                    # Extract the parent barcode if it exists
                    if m := re.search(r'[nN]o:\s*([0-9]+)', text):
                        data['param_parent_barcode'] = m.group(1)   
                        v = v.replace(m.group(0), '')
                        if not v.strip():
                            continue
                
                if param in data:
                    data[param] = f'{data[param]}; {v}'
                else:
                    data[param] = v
    
        return data    
            
    def parse(self, df):
        df[self.new_columns] = df[df[self.field_name].notna()][self.field_name].apply(self._params_to_columns)
        return df
        
if __name__ == "__main__":
    # parser = TextParser()
    # text = "Bollettino dei Musei e degli Istituti Biologici dell'Università di Genova ; vol. 68 (2003-2004)"
    # data = parser.parse(text)

    # parser = DateParser()
    # text = "Number 302-307 (1996)"
    # data = parser.parse(text)    

    parser = Params773Parser()

    data = [{
        'MMS Id': 991452102081,
        'Barcode': '440104',
        '773 - Local Param 06': '‡t Parasitology; ‡g Vol.112, Suppl.33 Pt.1-6 (1996); ‡w 9911338402081; ‡g no: 000421656'
    }]

    df = pd.DataFrame(data)

    df = parser.parse(df)
    

    print(df.to_dict(orient='records'))        

    

