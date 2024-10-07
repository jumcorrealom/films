from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, lit, current_timestamp

class BaseTransformer(ABC):

    def rename_columns(self, df: DataFrame, columns_to_rename: dict) -> DataFrame:
        for old_name, new_name in columns_to_rename.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def clean_numeric_columns(self, df: DataFrame, numeric_columns: list) -> DataFrame:
        for col_name in numeric_columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name).cast('string'), r'[^0-9.-]', ''))
            df = df.withColumn(col_name, when(col(col_name) == '', None).otherwise(col(col_name)))
            df = df.withColumn(col_name, col(col_name).cast('double'))
        return df

    def clean_id_columns(self, df: DataFrame, id_columns: list) -> DataFrame:
        for col_name in id_columns:
            df = df.withColumn(col_name, when(col(col_name) == 'NULL', None).otherwise(col(col_name)))
        return df

    def remove_non_numeric_chars(self, df: DataFrame, columns: list) -> DataFrame:
        for col_name in columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name).cast('string'), r'[^0-9]', ''))
        return df

    def replace_nulls(self, df: DataFrame, columns: list, null_values: list = ['NULL']) -> DataFrame:
        for column in columns:
            df = df.withColumn(column, when(
                col(column).isin(null_values), None
            ).otherwise(col(column)))
        return df
    
    
    def drop_duplicated(self, df: DataFrame) -> DataFrame:
        df = df.dropDuplicates()

        return df

    
    def write_to_parquet(self, df: DataFrame, output_path: str):
        df.write.mode('overwrite').parquet(output_path)

    

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
    
    def add_metadata(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        df = df.withColumn('source_file', lit(source_file))
        df = df.withColumn('sheet_name', lit(sheet_name))
        df = df.withColumn('processed_timestamp', current_timestamp())
        return df

    def write_to_parquet(self, df: DataFrame, output_path: str):
        df.write.mode('overwrite').parquet(output_path)

    def write_to_csv(self, df: DataFrame, output_path: str):
        try:
            df.coalesce(1).write.mode('overwrite').option('header', True).csv(output_path)
        except Exception as e:
            print(f"Failed to write CSV for {output_path}: {e}")
            raise