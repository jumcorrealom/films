from pyspark.sql import SparkSession
import pandas as pd
import os

def main():
    # Especificar la ruta de tu Python (ajusta según tu entorno)
    python_executable = os.path.join(os.environ['VIRTUAL_ENV'], 'Scripts', 'python.exe') if os.name == 'nt' else os.path.join(os.environ['VIRTUAL_ENV'], 'bin', 'python')

    # Crear la sesión de Spark
    spark = SparkSession.builder \
        .appName("FileReaderApp") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.pyspark.driver.python", python_executable) \
        .config("spark.pyspark.python", python_executable) \
        .getOrCreate()
    
    # Especificar el archivo a procesar
    file_path = "data/Films.xlsx"  # El archivo que estás usando
    
    xls = pd.ExcelFile(file_path, engine='openpyxl')
        
    # Diccionario para almacenar los DataFrames de PySpark
    dfs = {}
    
    print(xls.sheet_names)  # Mostrar todas las hojas disponibles en el archivo Excel
    
    # Iterar sobre todas las hojas y convertir cada una en un DataFrame de PySpark
    for sheet_name in xls.sheet_names:
        try:
            print(f"Intentando leer la hoja: {sheet_name}")
            # Leer cada hoja con pandas
            pdf = pd.read_excel(xls, sheet_name=sheet_name)
            print("Hoja leída con pandas")
            print(pdf.head())

            # Convertir el pandas DataFrame a PySpark DataFrame
            df = spark.createDataFrame(pdf)
            print(f"DataFrame de Spark creado para la hoja: {sheet_name}")
            
            # Almacenar el DataFrame en el diccionario
            dfs[sheet_name] = df
            
            # Mostrar los primeros registros del DataFrame
            df.show()

        except Exception as e:
            # Si ocurre un error al leer la hoja, se captura y continúa con la siguiente
            print(f"Error al leer la hoja '{sheet_name}': {e}")
            print("Saltando a la siguiente hoja...\n")
            continue
    
    # Devolver o hacer algo con el diccionario de DataFrames
    return dfs

if __name__ == "__main__":
    main()
