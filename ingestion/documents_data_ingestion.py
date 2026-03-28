import os
import logging
import boto3
from ingestion.utils.file_path_manager import FilePathManager

class DocumentDataIngestor:
    def __init__(self, s3_bucket: str = None, aws_access_key_id: str = None, aws_secret_access_key: str = None, aws_s3_endpoint: str = None):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', 
                                      aws_access_key_id=aws_access_key_id, 
                                      aws_secret_access_key=aws_secret_access_key,
                                      endpoint_url=aws_s3_endpoint)