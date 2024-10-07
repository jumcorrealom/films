
from pyspark.sql import SparkSession
from src.file_readers.file_reader_factory import FileReaderFactory
from src.processing.films_transformer import FilmsTransformer
from src.processing.inventory_transformer import InventoryTransformer
from src.processing.rental_transformer import RentalTransformer
from src.processing.customer_transformer import CustomerTransformer
from src.processing.store_transformer import StoreTransformer
from src.utils.spark_session_manager import SparkSessionManager
import os


def main():
    
    spark = SparkSessionManager.get_session()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    data_dir = os.path.join(project_root, "data")
    
    file_path = os.path.join(data_dir, "Films.xlsx")
    file_type = file_path.split('.')[-1]
    
    reader = FileReaderFactory.get_file_reader(file_type, spark)
    
    dfs = reader.read(file_path)
    
    transformers = {
        'film': FilmsTransformer(),
        'inventory': InventoryTransformer(),
        'rental': RentalTransformer(),
        'customer': CustomerTransformer(),
        'store': StoreTransformer()
    }
    
    for sheet_name, df in dfs.items():
        transformer = transformers.get(sheet_name)
        if transformer:
            df = transformer.transform(df, source_file=file_path, sheet_name=sheet_name)
            dfs[sheet_name] = df

            output_dir = os.path.join(data_dir, "processed_data", sheet_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{sheet_name}.parquet")

            transformer.write_to_parquet(df, output_path)
            
            print(f"Data for sheet '{sheet_name}' has been written to {output_path}")
        else:
            print(f"No transformer for sheet '{sheet_name}', skipping.")
    
    spark.stop()

if __name__ == "__main__":
    main()
