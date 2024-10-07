from pyspark.sql import DataFrame
from .base_transformer import BaseTransformer

class RentalTransformer(BaseTransformer):
    def transform(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        
        columns_to_rename = {
            ' rental_date': 'rental_date', 
            ' inventory_id': 'inventory_id',
            ' customer_id': 'customer_id',
            ' return_date': 'return_date',
            ' staff_id': 'staff_id',
            ' last_update': 'last_update'
        }
        df = self.rename_columns(df, columns_to_rename)
        
        df = self.replace_nulls(df, ['return_date'], null_values=['NULL', 'null', ''])
        df = self.add_metadata(df, source_file, sheet_name)
        return df
