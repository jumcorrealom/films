from pyspark.sql import SparkSession
from src.file_readers.file_reader_factory import FileReaderFactory
from src.processing.films_transformer import FilmsTransformer
from src.processing.inventory_transformer import InventoryTransformer
from src.processing.rental_transformer import RentalTransformer
from src.processing.customer_transformer import CustomerTransformer
from src.processing.store_transformer import StoreTransformer
import os
import sys

python_executable = sys.executable

# Set environment variables for PySpark
os.environ['PYSPARK_PYTHON'] = python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

def main():
    spark = SparkSession.builder \
        .appName("DataPipelineApp") \
        .getOrCreate()
    
    file_path = "data/Films.xlsx"
    file_type = file_path.split('.')[-1]
    
    # Get the appropriate reader
    reader = FileReaderFactory.get_file_reader(file_type, spark)
    
    # Read the Excel file
    dfs = reader.read(file_path)
    
    # Initialize transformers
    transformers = {
        'film': FilmsTransformer(),
        'inventory': InventoryTransformer(),
        'rental': RentalTransformer(),
        'customer': CustomerTransformer(),
        'store': StoreTransformer()
    }
    
    # Process each DataFrame
    for sheet_name, df in dfs.items():
        transformer = transformers.get(sheet_name)
        if transformer:
            df = transformer.transform(df)
            dfs[sheet_name] = df
            print(f"Transformed data for sheet '{sheet_name}':")
            df.show()
        else:
            print(f"No transformer for sheet '{sheet_name}', skipping.")
    
    # Optionally, write the transformed data to storage or proceed with further processing
    
    spark.stop()

if __name__ == "__main__":
    main()
