import csv
import logging
import os
from datetime import datetime
from io import StringIO
from time import sleep

from common.APIMixin import APIMixin
from common.JSONMixin import JSONMixin
from common.S3Utils import S3Utils
from common.LocalUtils import LocalUtils
from common.ActionName import ActionName
from common.Utils import Utils


class BaseLocation(JSONMixin, APIMixin):
    def __init__(self, event, parser_path, context):
        self.debug_next_step = False
        self.use_proxy = event["use_proxy"]
        super().__init__(self.use_proxy)
        self.running_in_lambda = False
        self.parser_folder = parser_path
        self.local_data_path = parser_path + "/data"
        self.input_file_path = "in"
        self.output_file_path = "location"
        self.failed_items_path = "failed_loc"
        self.status_path = "status"
        self.location_csv_path = f"{self.get_service_name()}_locations.csv"
        self.log_file = f"{self.get_service_name()}_locations.log"
        self.status_file = f".{self.get_service_name()}_locations.status"
        self.page_size = 1
        self.offset = 0
        self.append_log = []
        self.context = context

        if "log_id" in event:
            self.log_id = event["log_id"]
        else:
            self.log_id = datetime.now().strftime("%Y%m%d%H%M%S")

        if "page_size" in event:
            self.page_size = event["page_size"]

        if "offset" in event:
            self.offset = event["offset"]

        # Instantiate S3Utils or LocalUtils
        if "AWS_LAMBDA_FUNCTION_VERSION" in os.environ:
            print("in Lambda")
            self.bucket_name = "scrapers-resturantlambda"
            self.date_str = Utils(event).get_directory_name()
            self.version = self.date_str
            self.status_path = f"status/{self.date_str}/{self.get_service_name()}"
            self.input_file_path = f"in/{self.get_service_name()}"
            self.failed_items_path = f"failed_loc/{self.date_str}/{self.get_service_name()}"
            self.output_file_path = f"location/{self.date_str}/{self.get_service_name()}"
            self.running_in_lambda = True
            self.file_utils = S3Utils(self.bucket_name)
            # Only /tmp folder is writable in Lambda environment
            self.local_data_path = f"/tmp/{self.get_service_name()}"
            self.local_utils = LocalUtils(self.local_data_path)
            self.log_file_path = os.path.join(self.bucket_name, self.status_path)

            os.makedirs(self.local_data_path, exist_ok=True)
        else:
            print("In local")
            self.version = ""
            self.local_utils = LocalUtils(self.local_data_path)
            self.file_utils = self.local_utils
            self.log_file_path = os.path.join(self.file_utils.folder, self.status_path)

    def get_service_name(self):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def get_identifier_id(self, params):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def gen_location_preprocessor(self):
        pass

    def fetch_one_page(self, row, parent_id, size, offset):
        pass

    def url_page_size(self):
        return 50

    def gen_location(self):
        self.gen_location_preprocessor()
        last_id = self.offset

        list_items_csv = self.file_utils.read_file(self.input_file_path, self.location_csv_path)
        csv_reader = csv.DictReader(StringIO(list_items_csv))
        rows_list = list(csv_reader)
        row_parsed = 0
        all_parsed = True
        total_records = len(rows_list)

        logging.info(f"[{self.get_service_name()}] Generating Locations, Total records:{total_records}")

        for i, row in enumerate(rows_list):
            parent_id = self.get_identifier_id(row)
            offset = 0
            total = 10  # set for max for initial run
            size = self.url_page_size()

            if row_parsed >= last_id:
                # Read all record with help of pagination
                while total > offset:
                    if offset > 0:
                        logging.info(f"[{self.get_service_name()}] waiting for 1 sec, record:{i}")
                        sleep(1)
                    logging.info(f"[{self.get_service_name()}] Fetching record, identity:{parent_id}, offset:{offset}, page_size:{size}")
                    total = self.fetch_one_page(row, parent_id, size, offset)
                    offset = offset + size

            row_parsed += 1
            if row_parsed > last_id + 1:  # sleep if multiple items
                logging.info(f"[{self.get_service_name()}] waiting for 2 sec, record:{i}")
                sleep(2)

            if row_parsed >= last_id + self.page_size:
                logging.info(f"[{self.get_service_name()}] Records parsed: {i}, Total record: {total_records}")
                all_parsed = False
                break

            if self.get_remaining_time_sec() < 70:
                logging.info(
                    f"[{self.get_service_name()}][Menu]  Function is suspended because of possible timeout: {row_parsed}/{total_records}")
                all_parsed = False
                # self.page_size = self.offset - row_parsed
                break

        if row_parsed >= total_records:
            logging.info(f"[{self.get_service_name()}] All records parsed, Total record: {total_records}")
            all_parsed = True

        self.flush_log()

        percentage = str(round(row_parsed/total_records*100,2))+"%"

        if self.debug_next_step:
            # Move to new parser
            return {
                "parser": self.get_service_name(),
                "action": ActionName.PROCESS_MENU.value,
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": True,
                "completed": percentage,
                "version": self.version
            }

        if all_parsed:
            # Move to new parser
            return {
                "parser": self.get_service_name(),
                "action": ActionName.PROCESS_LOGS.value,
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": True,
                "completed": percentage,
                "version": self.version,
                "log_id": self.log_id,
                "log_file_path": self.log_file_path,
                "previous_action": ActionName.PROCESS_LOCATION.value
            }
            # return {
            #     "parser": self.get_service_name(),
            #     "action": ActionName.PROCESS_MENU.value,
            #     "use_proxy": self.use_proxy,
            #     "page_size": self.page_size,
            #     "offset": 0,
            #     "has_more": True,
            #     "completed": percentage,
            #     "version": self.version
            # }
        else:
            return {
                "parser": self.get_service_name(),
                "action": ActionName.PROCESS_LOCATION.value,
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": row_parsed,
                "has_more": True,
                "completed": percentage,
                "version": self.version,
                "log_id": self.log_id,
                "log_file": self.log_file_path
            }

    def append_to_log(self, content):
        content = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{ActionName.PROCESS_LOCATION.value},{self.log_id},{content}"
        self.append_log.append(content)

    def flush_log(self):
        content = '\n'.join(self.append_log)
        self.file_utils.append_to_file(self.status_path, self.log_file, content)

    def get_remaining_time_sec(self):
        if self.running_in_lambda:
            return self.context.get_remaining_time_in_millis() / 1000
        return 120
