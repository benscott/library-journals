import luigi
import yaml 

from journals.config import DATA_DIR, INPUT_DATA_DIR, PROCESSING_DATA_DIR






class AbbreviationsTask(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget(INPUT_DATA_DIR / 'abbrv.yml')
    
    def read(self):
        with self.output().open('r') as f:
            content = f.read()
            return yaml.safe_load(content)       

    def fields(self): 
        data = self.read()
        return {v.get('en') for v in data.values() if v.get('en')} | set(data.keys())


