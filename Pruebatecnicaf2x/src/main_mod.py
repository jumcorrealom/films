
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
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DataPipelineApp") \
        .getOrCreate()
    
    # Get the directory of the current script (main.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # The parent directory of src/ (i.e., pruebatecnicaf2x/)
    project_root = os.path.dirname(current_dir)
    
    # Path to the data directory
    data_dir = os.path.join(project_root, "data")
    
    # Input file path
    file_path = os.path.join(data_dir, "Films.xlsx")
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
            df = transformer.transform(df, source_file=file_path, sheet_name=sheet_name)
            dfs[sheet_name] = df

            # Define the output directory path
            output_dir = os.path.join(data_dir, "processed_data", sheet_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{sheet_name}.parquet")

            # After transforming the DataFrame
            print(f"Schema for sheet '{sheet_name}':")
            df.printSchema()
            print(f"Data sample for sheet '{sheet_name}':")
            df.show(5)

            # Write to Parquet locally
            #transformer.write_to_parquet(df, output_path)
            transformer.write_to_csv(df, output_path)
            
            print(f"Data for sheet '{sheet_name}' has been written to {output_path}")
        else:
            print(f"No transformer for sheet '{sheet_name}', skipping.")
    
    spark.stop()

if __name__ == "__main__":
    main()
