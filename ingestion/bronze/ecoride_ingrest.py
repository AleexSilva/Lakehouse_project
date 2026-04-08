from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.config_loader import ConfigLoader
from ingestion.utils.file_path_manager import FilePathManager
from ingestion.utils.data_ingestor import DataIngestor


def ingest_ecoride():
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)
    path_manager = FilePathManager(config.base_data_dir, config.lakehouse_s3_path)

    customer_file = path_manager.get_local_file_path("ecoride_customers", "csv")
    ingestor.ingest_file_to_bronze(customer_file, "ecoride", "customers", "csv")

    sales_file = path_manager.get_local_file_path("ecoride_sales", "csv")
    ingestor.ingest_file_to_bronze(sales_file, "ecoride", "sales", "csv")

if __name__ == "__main__":
    ingest_ecoride()