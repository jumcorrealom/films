from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class FileReader(ABC):
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self, file_path: str):
        pass
