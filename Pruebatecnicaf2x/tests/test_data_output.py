import unittest
from pyspark.sql import SparkSession
import os

class TestDataOutput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("DataPipelineTest") \
            .master("local[*]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_films_parquet(self):
        output_path = os.path.join("data", "processed_data", "Films", "Films.parquet")
        self.assertTrue(os.path.exists(output_path), f"{output_path} does not exist")
        
        df = self.spark.read.parquet(output_path)
        # Perform assertions on the DataFrame
        self.assertGreater(df.count(), 0, "DataFrame is empty")
        self.assertIn('title', df.columns, "'title' column is missing")
        # Add more assertions as needed
    
    def test_inventory_parquet(self):
        # Similar test for the 'inventory' sheet
        pass
    
    # Add tests for other sheets as needed

if __name__ == '__main__':
    unittest.main()
