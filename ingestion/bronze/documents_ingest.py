from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.config_loader import ConfigLoader
from ingestion.utils.file_path_manager import FilePathManager
from ingestion.utils.document_data_ingestor import DocumentDataIngestor
import os


def format_s3_bucket_name(s3_path):
    if s3_path.startswith("s3a://"):
        s3_path = s3_path[6:]
    return s3_path.rstrip('/')