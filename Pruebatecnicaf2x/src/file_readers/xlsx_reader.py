from src.file_readers.base_reader import FileReader

class XLSXFileReader(FileReader):
    
    def __init__(self, spark, tab_reader_strategy):
        super().__init__(spark)
        self.tab_reader_strategy = tab_reader_strategy
    
    def read(self, file_path: str):
        # Usar la estrategia para leer las pestañas del archivo
        return self.tab_reader_strategy.read_tabs(file_path, self.spark)
