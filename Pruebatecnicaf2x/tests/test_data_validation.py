import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_extract, when, to_timestamp, trim
from src.utils.spark_session_manager import SparkSessionManager
import os
import re







class TestDataValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSessionManager.get_session(app_name="DataValidationTest")
        # Ruta a los archivos Parquet procesados
        cls.processed_data_dir = os.path.join("data", "processed_data")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    def test_last_update_dates(self):
        # Leer el DataFrame procesado
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "film", "film.parquet"))
        
        # Eliminar espacios en blanco y convertir 'last_update' a tipo timestamp
        df = df.withColumn("last_update", to_timestamp(trim(col("last_update")), "yyyy-MM-dd HH:mm:ss"))
        
        # Filtrar filas con fechas inválidas (null)
        invalid_dates = df.filter(col("last_update").isNull())
        
        invalid_count = invalid_dates.count()
        
        if invalid_count == 0:
            print("Todas las fechas en 'last_update' están bien formateadas.")
        else:
            print(f"Se encontraron {invalid_count} fechas mal formateadas en 'last_update':")
            invalid_dates.show()
        
        self.assertEqual(invalid_count, 0, f"Hay {invalid_count} fechas mal formateadas en 'last_update'.")



    def test_inventory_numeric_columns(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "inventory", "inventory.parquet"))
        
        id_columns = ['inventory_id', 'film_id', 'store_id']
        
        for col_name in id_columns:
            # Verificar si todos los valores son numéricos
            non_numeric = df.filter(~col(col_name).rlike("^\d+$"))
            count_non_numeric = non_numeric.count()
            
            if count_non_numeric == 0:
                print(f"La columna '{col_name}' contiene únicamente caracteres numéricos.")
            else:
                print(f"La columna '{col_name}' contiene valores no numéricos:")
                non_numeric.show()
            
            self.assertEqual(count_non_numeric, 0, f"La columna '{col_name}' contiene valores no numéricos.")

    def test_rental_numeric_columns(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "rental", "rental.parquet"))
        
        id_columns = ['inventory_id', 'customer_id', 'staff_id']
        
        for col_name in id_columns:
            non_numeric = df.filter(~col(col_name).rlike("^\d+$"))
            count_non_numeric = non_numeric.count()
            
            if count_non_numeric == 0:
                print(f"La columna '{col_name}' contiene únicamente caracteres numéricos.")
            else:
                print(f"La columna '{col_name}' contiene valores no numéricos:")
                non_numeric.show()
            
            self.assertEqual(count_non_numeric, 0, f"La columna '{col_name}' contiene valores no numéricos.")


    def test_rental_date_columns(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "rental", "rental.parquet"))
        
        date_columns = ['rental_date', 'return_date', 'last_update']
        
        for col_name in date_columns:
            df = df.withColumn(col_name, to_timestamp(trim(col(col_name)), "yyyy-MM-dd HH:mm:ss"))
            invalid_dates = df.filter(col(col_name).isNull())
            count_invalid = invalid_dates.count()
            
            if count_invalid == 0:
                print(f"La columna '{col_name}' contiene únicamente fechas válidas.")
            else:
                print(f"Valores no válidos o mal formateados en la columna '{col_name}':")
                invalid_dates.select(col_name).show()
            
            self.assertEqual(count_invalid, 0, f"Hay {count_invalid} fechas mal formateadas en '{col_name}'.")



    def test_customer_numeric_columns(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "customer", "customer.parquet"))
        
        id_columns = ['customer_id', 'store_id', 'address_id', 'active']
        
        for col_name in id_columns:
            non_numeric = df.filter(~col(col_name).rlike("^\d+$"))
            count_non_numeric = non_numeric.count()
            
            if count_non_numeric == 0:
                print(f"La columna '{col_name}' contiene únicamente caracteres numéricos.")
            else:
                print(f"La columna '{col_name}' contiene valores no numéricos:")
                non_numeric.show()
            
            self.assertEqual(count_non_numeric, 0, f"La columna '{col_name}' contiene valores no numéricos.")


    def test_customer_email_column(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "customer", "customer.parquet"))
        
        email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        
        invalid_emails = df.filter(~col('email').rlike(email_regex))
        count_invalid_emails = invalid_emails.count()
        
        if count_invalid_emails == 0:
            print("La columna 'email' contiene únicamente correos electrónicos válidos.")
        else:
            print("La columna 'email' contiene correos electrónicos no válidos:")
            invalid_emails.select('email').show()
        
        self.assertEqual(count_invalid_emails, 0, f"Hay {count_invalid_emails} correos electrónicos no válidos en 'email'.")

    def test_customer_names(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "customer", "customer.parquet"))
        
        valid_name_regex = r'^[a-zA-Z\s]+$'
        
        # Verificar 'first_name'
        invalid_first_names = df.filter(~col('first_name').rlike(valid_name_regex))
        count_invalid_first = invalid_first_names.count()
        
        # Verificar 'last_name' (permitir nulos o vacíos)
        invalid_last_names = df.filter((~col('last_name').rlike(valid_name_regex)) & (col('last_name').isNotNull()))
        count_invalid_last = invalid_last_names.count()
        
        if count_invalid_first == 0 and count_invalid_last == 0:
            print("Todos los nombres y apellidos son válidos.")
        else:
            if count_invalid_first > 0:
                print(f"Se encontraron {count_invalid_first} nombres no válidos:")
                invalid_first_names.select('first_name').show()
            if count_invalid_last > 0:
                print(f"Se encontraron {count_invalid_last} apellidos no válidos:")
                invalid_last_names.select('last_name').show()
        
        self.assertEqual(count_invalid_first, 0, f"Hay {count_invalid_first} nombres no válidos en 'first_name'.")
        self.assertEqual(count_invalid_last, 0, f"Hay {count_invalid_last} apellidos no válidos en 'last_name'.")



    def test_customer_date_columns(self):
        df = self.spark.read.parquet(os.path.join(self.processed_data_dir, "customer", "customer.parquet"))
        
        date_columns = ['create_date', 'last_update']
        
        for col_name in date_columns:
            df = df.withColumn(col_name, to_timestamp(trim(col(col_name)), "yyyy-MM-dd HH:mm:ss"))
            invalid_dates = df.filter(col(col_name).isNull())
            count_invalid = invalid_dates.count()
            
            if count_invalid == 0:
                print(f"La columna '{col_name}' contiene únicamente fechas válidas.")
            else:
                print(f"Valores no válidos o mal formateados en la columna '{col_name}':")
                invalid_dates.select(col_name).show()
            
            self.assertEqual(count_invalid, 0, f"Hay {count_invalid} fechas mal formateadas en '{col_name}'.")



