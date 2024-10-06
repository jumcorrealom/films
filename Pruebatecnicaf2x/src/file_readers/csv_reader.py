from src.file_readers.base_reader import FileReader

class CSVFileReader(FileReader):
    
    def read(self, file_path: str):
        return self.spark.read.csv(file_path, header=True, inferSchema=True)
