import json
import logging
import os
from datetime import datetime

from common.APIMixin import APIMixin
from common.ActionName import ActionName
from common.JSONMixin import JSONMixin
from common.LocalUtils import LocalUtils
from common.S3Utils import S3Utils
from common.Utils import Utils


class BasePostMenu(JSONMixin, APIMixin):
    def __init__(self, event, parser_path, context):
        self.use_proxy = event["use_proxy"]
        super().__init__(self.use_proxy)

        self.append_log = []
        self.status_path = "status"
        self.log_file = f"{self.get_service_name()}_post_menu.log"

        self.running_in_lambda = False
        self.local_data_path = parser_path + "/data"
        self.input_file_path = "menu"
        self.output_file_path = "post-menu"
        self.cost_file_path = "post-menu-cost"
        self.context = context

        self.offset = 0
        self.page_size = 10
        self.offset_end = -1

        if "page_size" in event:
            self.page_size = event["page_size"]


        if "offset" in event:
            self.offset = event["offset"]

        if "offset_end" in event:
            self.offset_end = event["offset_end"]

        # Instantiate S3Utils or LocalUtils
        if "AWS_LAMBDA_FUNCTION_VERSION" in os.environ:
            print("in Lambda")
            self.bucket_name = "scrapers-resturantlambda"
            self.date_str = Utils(event).get_directory_name()
            self.version = self.date_str
            self.input_file_path = f"menu/{self.date_str}/{self.get_service_name()}"
            self.output_file_path = f"post-menu/{self.date_str}/{self.get_service_name()}"
            self.cost_file_path = f"post-menu-cost/{self.date_str}/{self.get_service_name()}"
            self.status_path = f"status/{self.date_str}/{self.get_service_name()}"
            self.running_in_lambda = True
            self.file_utils = S3Utils(self.bucket_name)
            # Only /tmp folder is writable in Lambda environment
            self.local_data_path = "/tmp"
            self.local_utils = LocalUtils(self.local_data_path)
        else:
            print("In local")
            self.version = ""
            self.local_utils = LocalUtils(self.local_data_path)
            self.file_utils = self.local_utils

    def associate_missing_price(self):
        all_files = self.file_utils.list(self.input_file_path)
        total_records = len(all_files)
        if self.offset_end == -1:
            self.offset_end = total_records + 1
        row_parsed = 0
        all_parsed = False
        stop_process = False

        for f_name in all_files:
            if row_parsed >= self.offset:
                if self.running_in_lambda:
                    self.file_utils.download_object(os.path.join(self.input_file_path, f_name),
                                                    os.path.join(self.local_data_path, self.input_file_path, f_name))

                item_id = f_name.split(".")[0]
                try:
                    item_details_json = self.read_from_json_file(os.path.join(self.local_data_path,
                                                                              self.input_file_path, f_name))
                    if item_details_json is not None:
                        self.read_menu( item_details_json, item_id, f_name)
                    else:
                        logging.error('Data was not inputted for %s', item_id)
                except Exception as e:
                    logging.error(
                        'Data was not inputted for %s because of Error: %s', item_id, e)
            row_parsed += 1

            if row_parsed >= self.offset + self.page_size:
                logging.info(f"[{self.get_service_name()}] Menu parsed: {row_parsed}/{total_records}")
                break

            if self.get_remaining_time_sec() < 70:
                logging.info(
                    f"[{self.get_service_name()}] Function is suspended because of possible timeout: {row_parsed}/{total_records}")
                # self.page_size = self.offset - row_parsed
                break

        if row_parsed >= self.offset_end:
            logging.info(
                f"[{self.get_service_name()}] All post-menu items between ({self.offset}, {self.offset_end}) are parsed, Total record: {total_records}")
            stop_process = True

        if row_parsed >= total_records:
            logging.info(f"[{self.get_service_name()}] All menu items are parsed, Total record: {total_records}")
            all_parsed = True

        self.flush_log()

        percentage = str(round(row_parsed / total_records * 100, 2)) + "%"

        if all_parsed:
            return {
                "parser": self.get_service_name(),
                "action": ActionName.MAKE_CSV.value,
                "use_proxy": False,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": True,
                "offset_end": self.offset_end,
                "completed": percentage,
                "version": self.version,
            }
        else:
            if stop_process:
                return {
                    "parser": self.get_service_name(),
                    "action": "None",
                    "use_proxy": False,
                    "page_size": self.page_size,
                    "offset": row_parsed,
                    "has_more": False,
                    "offset_end": self.offset_end,
                    "completed": percentage,
                    "version": self.version,
                }
            else:
                return {
                    "parser": self.get_service_name(),
                    "action": ActionName.PROCESS_POST_MENU.value,
                    "use_proxy": False,
                    "page_size": self.page_size,
                    "offset": row_parsed,
                    "has_more": True,
                    "offset_end": self.offset_end,
                    "completed": percentage,
                    "version": self.version,
                }



    def read_menu(self, api_response, menu_id, source_name):
        # file_locations = []
        store_detail = api_response['store']
        menu_details = api_response['menu_detail']

        menu_details, file_locations = self.parse_items(menu_details, menu_id)

        try:
            # update the source menu file to record the cost file location
            menu_data = {"store": store_detail, "menu_detail": menu_details, "cost_file":file_locations}
            self.file_utils.write_file(self.output_file_path, source_name, json.dumps(menu_data))
            logging.info(f"[{self.get_service_name()}] successfully write:{source_name} to post-menu file")
        except Exception as e:
            logging.error(f'[{self.get_service_name()}] File %s: Error:%s', menu_id, e)

        return None

    def get_remaining_time_sec(self):
        if self.running_in_lambda:
            return self.context.get_remaining_time_in_millis() / 1000
        return 120

    def append_to_log(self, content):
        content = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},process.post-menu,{content}"
        self.append_log.append(content)

    def flush_log(self):
        content = '\n'.join(self.append_log)
        self.file_utils.append_to_file(self.status_path, self.log_file, content)

    def get_service_name(self):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def parse_items(self, menu_details, menu_id):
        raise NotImplementedError("Subclasses should implement this method")

    # def read_menu(self, api_response, store_id, source_name):
    #     raise NotImplementedError("Subclasses should implement this method")

    def gen_request(self, menu_id):
        raise NotImplementedError("Subclasses should implement this method")
