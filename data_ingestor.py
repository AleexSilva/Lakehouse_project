from pyspark.sql import SparkSession
import logging

class DataIngestor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_or_replace_iceberg_table(self, df, layer, business_entity, table_name):
        # Generate SQL statement for table creation
        schema = df.schema
        schema_sql = ', '.join([f"{field.name} {field.dataType.simpleString()}" for field in schema.fields])
        create_table_sql = f"CREATE OR REPLACE TABLE nessie.{layer}.{business_entity}.{table_name} ({schema_sql}) USING iceberg"

        # Execute the SQL statement
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{layer};")
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{layer}.{business_entity};")
            self.spark.sql(create_table_sql)
            logging.info(f"Iceberg table nessie.{layer}.{business_entity}.{table_name} created/replaced successfully.")
        except Exception as e:
            logging.error(f"Error creating/replacing Iceberg table: {e}")
            raise e