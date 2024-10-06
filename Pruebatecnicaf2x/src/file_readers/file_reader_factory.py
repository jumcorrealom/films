from src.file_readers.csv_reader import CSVFileReader
from src.file_readers.json_reader import JSONFileReader
from src.file_readers.xlsx_reader import XLSXFileReader
from src.file_readers.all_tabs_reader_strategy import AllTabsReaderStrategy

class FileReaderFactory:
    
    @staticmethod
    def get_file_reader(file_type: str, spark):
        if file_type == 'csv':
            return CSVFileReader(spark)
        elif file_type == 'json':
            return JSONFileReader(spark)
        elif file_type == 'xlsx':
            return XLSXFileReader(spark, AllTabsReaderStrategy())
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
