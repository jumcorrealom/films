from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
from .base_transformer import BaseTransformer

class CustomerTransformer(BaseTransformer):
    def transform(self, df: DataFrame, source_file: str, sheet_name: str) -> DataFrame:
        # Rename Columns
        columns_to_rename = {
            ' store_id': 'store_id', 
            ' first_name': 'first_name',
            ' last_name': 'last_name',
            ' email': 'email',
            ' address_id': 'address_id',
            ' active': 'active',
            ' create_date': 'create_date',
            ' last_update': 'last_update',
            ' customer_id_old': 'customer_id_old',
            ' segment': 'segment'
        }
        df = self.rename_columns(df, columns_to_rename)
    
        def clean_email(email):
            email = re.sub(r'\s+', '', email or '')
            
            valid_email_regex = r'^[a-zA-Z0-9._-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,3}$'
            
            if not re.match(valid_email_regex, email):
                parts = email.split('@')
                username = re.sub(r'[^a-zA-Z0-9._-]', '', parts[0]) if len(parts) > 0 else ''
                domain = re.sub(r'[^a-zA-Z0-9.-]', '', parts[1]) if len(parts) > 1 else ''
                domain = re.sub(r'(\.[a-zA-Z]{2,3})[^\s]*$', r'\1', domain)
                email = username + '@' + domain
            return email
        clean_email_udf = udf(clean_email, StringType())
        df = df.withColumn('email', clean_email_udf(col('email')))
        
        def clean_name(name, is_last_name=False):
            name = (name or '').strip()
            name = re.sub(r'-', ' ', name)
            if is_last_name:
                name = re.sub(r'\\([a-zA-Z])', r' \1', name)
            valid_name_regex = r'^[a-zA-Z\s]+$'
            if not re.match(valid_name_regex, name):
                name = re.sub(r'[^a-zA-Z\s]', '', name)
            return name
        clean_first_name_udf = udf(lambda x: clean_name(x, False), StringType())
        clean_last_name_udf = udf(lambda x: clean_name(x, True), StringType())
        df = df.withColumn('first_name', clean_first_name_udf(col('first_name')))
        df = df.withColumn('last_name', clean_last_name_udf(col('last_name')))
        df = self.add_metadata(df, source_file, sheet_name)
        return df
