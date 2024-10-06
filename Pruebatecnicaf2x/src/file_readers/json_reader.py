from src.file_readers.base_reader import FileReader

class JSONFileReader(FileReader):
    
    def read(self, file_path: str):
        return self.spark.read.json(file_path)
