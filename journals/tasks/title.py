import pandas as pd
from pathlib import Path
import luigi
import recordlinkage

from journals.config import PROCESSING_DATA_DIR, logger
from journals.compare import ComparePartialRatio, process_pool_compare



class TitleComparisonTask(luigi.Task):

    child_task = luigi.TaskParameter()
    parent_task = luigi.TaskParameter()

    def requires(self):

        return [
            self.child_task,
            self.parent_task
        ]    
    
    def run(self):

        child_df = pd.read_pickle(self.child_task.output().path)
        parent_df = pd.read_pickle(self.parent_task.output().path) 

        # parent_df = parent_df[:10000]

        logger.info(f'Aligning {len(child_df)} child records to  {len(parent_df)} parents') 

        logger.info(f'Comparing datasets by title') 
        df = self._compare_title(child_df, parent_df)
        logger.info(f'Title comparison complete {len(df)} features')         

        df.to_pickle(self.output().path)

    def _compare_title(self, child_df, parent_df):

        indexer = recordlinkage.Index()
        indexer.full()

        candidate_links = indexer.index(child_df, parent_df)
            
        compare = recordlinkage.Compare()

        compare.string(
            "title", "title", method="jarowinkler", label="title_jaro"
        )

        compare.add(ComparePartialRatio('title', 'title', label='title_partial_ratio'))

        features = process_pool_compare(candidate_links, compare, child_df, parent_df)
        return features  

    def output(self):
        child_task_filename = Path(self.child_task.output().path).stem
        parent_task_filename = Path(self.parent_task.output().path).stem
        output_file_name = f"{child_task_filename}-{parent_task_filename}-title-features.pkl"
        return luigi.LocalTarget(PROCESSING_DATA_DIR / output_file_name)    