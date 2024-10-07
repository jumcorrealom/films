from pyspark.sql import DataFrame
from .base_transformer import BaseTransformer

class InventoryTransformer(BaseTransformer):
    def transform(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        
        columns_to_rename = {
            ' store_id': 'store_id', 
            ' last_update': 'last_update'
        }
        df = self.rename_columns(df, columns_to_rename)
        
        df = self.remove_non_numeric_chars(df, ['store_id'])
        df = self.add_metadata(df, source_file, sheet_name)
        return df
