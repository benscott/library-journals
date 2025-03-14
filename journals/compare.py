import pandas as pd
from rapidfuzz.fuzz import partial_ratio
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from recordlinkage.base import BaseCompareFeature


from journals.config import PROCESSING_DATA_DIR
from journals.config import logger


class CompareSet(BaseCompareFeature):

    @staticmethod
    def _compare(x, y):
        # If we don't have a value for both columns, return 0
        if not x or not y:
            return 0
        # We have data so if the intersection return 1
        if len(x.intersection(y)):
            return 1
            
        return -1
        
    def _compute_vectorized(self, s1, s2):
        return s1.combine(s2, self._compare)
    
class ComparePartialRatio(BaseCompareFeature):

    def _compute_vectorized(self, s1, s2):
        return s1.combine(s2, lambda x, y: int(partial_ratio(x, y)))


def process_chunk(i, chunk_size, candidate_links, compare, child_df, parent_df):
    chunk = candidate_links[i:i+chunk_size]    
    result = compare.compute(chunk, child_df, parent_df)
    return result
            
def process_pool_compare(candidate_links, compare, child_df, parent_df):
    final_results = []
    chunk_size = 10000
    
    for i in tqdm(range(0, len(candidate_links), chunk_size), desc="Processing Chunks"):
        result = process_chunk(i, chunk_size, candidate_links, compare, child_df, parent_df)
        final_results.append(result)

    logger.info(f'Comparison complete') 

    return pd.concat(final_results)
