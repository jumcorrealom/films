from pyspark.sql import SparkSession
import sys
import os

class SparkSessionManager:
    _instance = None

    @staticmethod
    def get_session(app_name="DataPipelineApp"):
        if SparkSessionManager._instance is None:
            python_executable = sys.executable

            # Establecer las variables de entorno para PySpark
            os.environ['PYSPARK_PYTHON'] = python_executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

            SparkSessionManager._instance = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .getOrCreate()
        return SparkSessionManager._instance
