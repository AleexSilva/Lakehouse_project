import os

class FilePathManager:
    def __init__(self, base_data_dir, lakehouse_s3_path):
        self.base_data_dir = base_data_dir
        self.lakehouse_s3_path = lakehouse_s3_path

    def get_local_file_path(self, dataset_name, file_type):
        file_name = f"{dataset_name}.{file_type}"
        return os.path.join(self.base_data_dir, file_name)
