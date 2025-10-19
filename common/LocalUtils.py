import os

from botocore.exceptions import ClientError

from common.S3Utils import S3Utils


# LocalUtil class to simulate local file system operations
class LocalUtils(S3Utils):
    def __init__(self, folder):
        super().__init__(folder)
        self.folder = folder

    def list(self, local_path):
        key = str(os.path.join(self.folder, local_path))
        from os import listdir
        only_files = [f for f in listdir(key) if os.path.isfile(os.path.join(key, f))]
        return only_files

    def file_exists(self, local_path, file_name):
        try:
            key = os.path.join(self.folder, local_path, file_name)
            if os.path.isfile(key):
                return True
            else:
                return False
        except ClientError:
            return False

    def read_file(self, local_path, file_name):
        key = os.path.join(self.folder, local_path, file_name)
        with open(key, 'r') as file:
            return file.read()

    def write_file(self, local_path, file_name, content):
        key = os.path.join(self.folder, local_path, file_name)
        os.makedirs(os.path.dirname(key), exist_ok=True)
        with open(key, 'w', newline='', encoding='utf-8') as file:
            file.write(content)

    def append_to_file(self, local_path, file_name, content):
        key = os.path.join(self.folder, local_path, file_name)
        os.makedirs(os.path.dirname(key), exist_ok=True)
        with open(key, 'a', newline='', encoding='utf-8') as file:
            file.write(content)
            file.write("\n")
