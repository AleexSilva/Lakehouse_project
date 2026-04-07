from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.config_loader import ConfigLoader
from ingestion.utils.file_path_manager import FilePathManager
from ingestion.utils.document_data_ingestor import DocumentDataIngestor
import os


def format_s3_bucket_name(s3_path):
    if s3_path.startswith("s3a://"):
        s3_path = s3_path[6:]
    return s3_path.rstrip('/')

def ingest_documents():
    config = ConfigLoader()
    ingestor = DocumentDataIngestor(
        s3_bucket=format_s3_bucket_name(config.lakehouse_s3_path),
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        aws_s3_endpoint=config.aws_s3_endpoint
    )