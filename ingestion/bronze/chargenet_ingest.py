from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.data_ingestor import DataIngestor
from ingestion.utils.config_loader import ConfigLoader
from ingestion.utils.file_path_manager import FilePathManager

def ingest_chargenet():
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)
    

if __name__ == "__main__":
    ingest_chargenet()