from pyspark.sql import DataFrame
from .base_transformer import BaseTransformer

class FilmsTransformer(BaseTransformer):
    def transform(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        
        columns_to_rename = {
            ' title': 'title', 
            ' description': 'description', 
            ' release_year': 'release_year', 
            ' language_id': 'language_id', 
            ' original_language_id': 'original_language_id',
            ' rental_duration': 'rental_duration',
            ' rental_rate': 'rental_rate',
            ' length': 'length',
            ' replacement_cost': 'replacement_cost',
            ' num_voted_users': 'num_voted_users',
            ' rating': 'rating',
            ' special_features': 'special_features'
        }
        df = self.rename_columns(df, columns_to_rename)
        
        id_columns = ['film_id', 'title', 'description', 'language_id', 'original_language_id',
                      'rating', 'special_features']
        df = self.clean_id_columns(df, id_columns)
        
        numeric_columns = ['release_year', 'rental_duration', 'rental_rate', 'length',
                           'replacement_cost', 'num_voted_users']
        df = self.clean_numeric_columns(df, numeric_columns)
        
        df = df.drop('original_language_id')
        
        df = self.remove_non_numeric_chars(df, ['film_id'])

        df = self.drop_duplicated(df)

        df = self.add_metadata(df, source_file, sheet_name)
        return df
