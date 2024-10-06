import pandas as pd
from src.file_readers.tab_reader_strategy import TabReaderStrategy

class AllTabsReaderStrategy(TabReaderStrategy):
    
    def read_tabs(self, file_path: str, spark):
        # Leer todas las pestañas del archivo Excel usando pandas
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        
        # Diccionario para almacenar los DataFrames de PySpark
        dfs = {}
        
        # Iterar sobre todas las hojas y convertir cada una en un DataFrame de PySpark
        for sheet_name in xls.sheet_names[1:]:
            pdf = pd.read_excel(xls, sheet_name=sheet_name)
            df = spark.createDataFrame(pdf)
            dfs[sheet_name] = df
        
        return dfs
