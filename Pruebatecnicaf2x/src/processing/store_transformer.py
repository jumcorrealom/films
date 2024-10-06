from pyspark.sql import DataFrame
from .base_transformer import BaseTransformer

class StoreTransformer(BaseTransformer):
    def transform(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        # Rename Columns
        columns_to_rename = {
            ' address_id': 'address_id', 
            ' last_update': 'last_update'
        }
        df = self.rename_columns(df, columns_to_rename)
        df = self.add_metadata(df, source_file, sheet_name)
        return df
