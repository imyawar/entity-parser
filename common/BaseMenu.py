import csv
import json
import logging
import os
from datetime import datetime
from io import StringIO
from time import sleep

from common.APIMixin import APIMixin
from common.ActionName import ActionName
from common.JSONMixin import JSONMixin
from common.LocalUtils import LocalUtils
from common.ParserName import ParserName
from common.S3Utils import S3Utils
from common.Utils import Utils


class BaseMenu(JSONMixin, APIMixin):
    def __init__(self, event, parser_path, context):
        self.use_proxy = event["use_proxy"]
        super().__init__(self.use_proxy)
        self.running_in_lambda = False
        self.cbsa_path_json = parser_path+"/../data/cbsa_bounding_boxes.json"

        self.local_data_path = parser_path + "/data"
        self.input_file_path = "location"
        self.output_file_path = "menu"
        self.failed_items_path = "failed_menu"
        self.status_path = "status"
        self.log_file = f"{self.get_service_name()}_menu.log"
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

        self.offset_end = -1
        self.force_fetch = False
        self.goto_next_step = True
        if "log_id" in event:
            self.log_id = event["log_id"]
        else:
            self.log_id = datetime.now().strftime("%Y%m%d%H%M%S")
        if "offset_end" in event:
            self.offset_end = event["offset_end"]

        if "force_fetch" in event:
            self.force_fetch = event["force_fetch"]

        if "goto_next_step" in event:
            self.goto_next_step = event["goto_next_step"]

        # Instantiate S3Utils or LocalUtils
        if "AWS_LAMBDA_FUNCTION_VERSION" in os.environ:
            print("in Lambda")
            self.bucket_name = "plotresin"
            self.date_str = Utils(event).get_directory_name()
            self.version = self.date_str
            self.input_file_path = f"location/{self.date_str}/{self.get_service_name()}"
            self.failed_items_path = f"failed_menu/{self.date_str}/{self.get_service_name()}"
            self.output_file_path = f"menu/{self.date_str}/{self.get_service_name()}"
            self.status_path = f"status/{self.date_str}/{self.get_service_name()}"
            self.running_in_lambda = True
            self.file_utils = S3Utils(self.bucket_name)
            # Only /tmp folder is writable in Lambda environment
            self.local_data_path = f"/tmp/{self.get_service_name()}"
            self.local_utils = LocalUtils(self.local_data_path)
            self.log_file_path = os.path.join(self.bucket_name,self.status_path)

            os.makedirs(self.local_data_path, exist_ok=True)
        else:
            print("In local")
            self.version = ""
            self.local_utils = LocalUtils(self.local_data_path)
            self.file_utils = self.local_utils
            self.log_file_path = os.path.join(self.file_utils.folder,self.status_path)

    def get_service_name(self):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def get_store_id(self, params):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def parse_location_for_menu(self, item_details_json, filename):
        raise NotImplementedError("Subclasses should implement this method")

    def gen_request(self, params):
        raise NotImplementedError("Subclasses should implement this method")

    def __read_location_json(self, filename):
        if self.running_in_lambda:
            if self.file_utils.file_exists(self.input_file_path, filename):
                print("Downloading from s3: " + str(os.path.join(self.input_file_path, filename)))
                self.file_utils.download_object(os.path.join(self.input_file_path, filename),
                                                os.path.join(self.local_data_path, self.input_file_path, filename))

        item_details_json = self.read_from_json_file(os.path.join(
            self.local_data_path, self.input_file_path, filename))

        return self.parse_location_for_menu(item_details_json, filename)

    def generate_files_list(self, file_name):
        logging.info(f"[{self.get_service_name()}] Start: Generating filename:{file_name}")
        all_files = self.file_utils.list(self.input_file_path)
        logging.info(f"[{self.get_service_name()}] Start: Total files found:{len(all_files)}")

        if self.running_in_lambda:
            key = os.path.join(self.local_data_path, file_name)
        else:
            key = os.path.join(self.local_data_path, self.status_path, file_name)
            try:
                os.remove(key)
            except OSError:
                pass
            os.makedirs(os.path.dirname(key), exist_ok=True)

        with open(key, 'a', newline='',
                  encoding='utf-8') as csv_out_file:
            writer = csv.DictWriter(csv_out_file, fieldnames=['id', 'file_name'])
            writer.writeheader()
            count = 0
            for row in all_files:
                if row.endswith(".json"):
                    count = count + 1
                    writer.writerow({'id': count, 'file_name': row})

        if self.running_in_lambda:
            source_file = os.path.join(self.local_data_path, file_name)
            dest_file = os.path.join(self.status_path, file_name)
            logging.info(f"[{self.get_service_name()}] Uploading to S3: source:{source_file}, destination:{dest_file}")
            self.file_utils.upload_object(source_file, dest_file)

        self.append_to_log(f"{self.log_id},generate_files_list,file_count,{count},success")

    def gen_menu(self):
        if self.offset == 0 or not self.file_utils.file_exists(self.status_path, "all_branches.csv"):
            self.generate_files_list("all_branches.csv")

        list_items_csv = self.file_utils.read_file(self.status_path, "all_branches.csv")
        csv_reader = csv.DictReader(StringIO(list_items_csv))
        rows_list = list(csv_reader)
        row_parsed = 0
        all_parsed = False
        total_records = len(rows_list)
        if self.offset_end == -1:
            self.offset_end = total_records+10

        for i, row in enumerate(rows_list):
            if row_parsed >= self.offset:
                j_filename = row["file_name"]
                store_data = self.__read_location_json(j_filename)
                store_data['scrape_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # store_data_cbsa = self.attach_cbsa(store_data)
                # cbsa= self.find_cbsa(store_data['latitude'], store_data['longitude'])
                # if cbsa is not None:
                #     store_data['CBSAFP']=cbsa['CBSAFP']
                #     store_data['GEOID'] = cbsa['GEOID']
                #     store_data['CSAFP'] = cbsa['CSAFP']
                # else:
                #     store_data['CBSAFP']= '0'
                #     store_data['GEOID'] = '0'
                #     store_data['CSAFP'] = '0'

                if self.force_fetch or not self.file_utils.file_exists(self.output_file_path, j_filename):
                    logging.info(f"[{self.get_service_name()}] Start: processing record:{i}, filename:{j_filename}")
                    item_id, menu_details = self.gen_request(store_data)

                    if not self.goto_next_step:
                        logging.info(
                            f"[{self.get_service_name()}][Menu] Function is suspended because of cookie expiration and zenRows API failure")
                        break

                    if menu_details:
                        if self.get_service_name() == ParserName.popeyes.name:
                            menu_data = {"store": store_data, "menu_detail": menu_details}
                        elif self.get_service_name() == ParserName.solidcore.name:
                            menu_data = {"store": store_data, "menu_detail": menu_details}
                        else:
                            menu_data = {"store": store_data, "menu_detail": menu_details.json()}
                        self.file_utils.write_file(self.output_file_path, j_filename, json.dumps(menu_data))
                        self.append_to_log(f"{self.log_id},url,{j_filename},{self.offset},success")
                        logging.info(
                            f"[{self.get_service_name()}] Success: processing record:{i}, store:{item_id}, filename:{j_filename}")
                    else:
                        self.file_utils.write_file(self.failed_items_path, j_filename, "None")
                        self.append_to_log(f"{self.log_id},url,{j_filename},{self.offset},failure")
                        logging.error(
                            f"[{self.get_service_name()}] Error: processing record:{i}, store:{item_id}, filename:{j_filename}")

                    if row_parsed > self.offset + 1:  # sleep if multiple items
                        logging.info(f"[{self.get_service_name()}] waiting for 2 sec, record:{i}")
                        sleep(2)
                else:
                    content = f"{self.log_id},file,{j_filename},found,success"
                    self.append_to_log(content)

            row_parsed += 1


            if row_parsed >= self.offset + self.page_size:
                logging.info(f"[{self.get_service_name()}] Menu parsed: {i}, Total record: {total_records}")
                break

            if self.get_remaining_time_sec() < 70:
                logging.info(
                    f"[{self.get_service_name()}][Menu] Function is suspended because of possible timeout: {row_parsed}/{total_records}")
                # self.page_size = self.offset - row_parsed
                break

        if row_parsed >= self.offset_end:
            logging.info(f"[{self.get_service_name()}] All menu items between ({self.offset}, {self.offset_end}) are parsed, Total record: {total_records}")
            all_parsed = True

        if row_parsed >= total_records:
            logging.info(f"[{self.get_service_name()}] All menu items are parsed, Total record: {total_records}")
            all_parsed = True

        self.flush_log()

        percentage = str(round(row_parsed/total_records*100,2))+"%"


        if not self.goto_next_step:
            # Move to next_step
            return {
                "parser": self.get_service_name(),
                "action": 'None',
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": False,
                "offset_end": self.offset_end,
                "force_fetch": self.force_fetch,
                "goto_next_step": self.goto_next_step,
                "log_id": self.log_id,
            }

        if all_parsed:
            # next_action = ActionName.MAKE_CSV.value
            # if (self.get_service_name() == ParserName.hardees.name
            #         or self.get_service_name() == ParserName.cjr.name
            #         or self.get_service_name() == ParserName.popeyes.name):
            #     next_action = ActionName.PROCESS_POST_MENU.value
            # return {
            #     "parser": self.get_service_name(),
            #     "action": next_action,
            #     "use_proxy": self.use_proxy,
            #     "page_size": self.page_size,
            #     "offset": 0,
            #     "has_more": True,
            #     "offset_end": self.offset_end,
            #     "force_fetch": self.force_fetch,
            #     "goto_next_step": self.goto_next_step,
            #     "completed": percentage,
            #     "version": self.version,
            #     "log_id": self.log_id
            # }

            next_action = ActionName.PROCESS_LOGS.value
            return {
                "parser": self.get_service_name(),
                "action": next_action,
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": True,
                "offset_end": self.offset_end,
                "force_fetch": self.force_fetch,
                "goto_next_step": self.goto_next_step,
                "completed": percentage,
                "version": self.version,
                "log_id": self.log_id,
                "log_file_path": self.log_file_path,
                "previous_action": ActionName.PROCESS_MENU.value
            }
        else:
            return {
                "parser": self.get_service_name(),
                "action": ActionName.PROCESS_MENU.value,
                "use_proxy": self.use_proxy,
                "page_size": self.page_size,
                "offset": row_parsed,
                "has_more": True,
                "offset_end": self.offset_end,
                "force_fetch": self.force_fetch,
                "goto_next_step": self.goto_next_step,
                "completed": percentage,
                "version": self.version,
                "log_id": self.log_id
            }

    def append_to_log(self, content):
        content = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{ActionName.PROCESS_MENU.value},{content}"
        self.append_log.append(content)

    def flush_log(self):
        content = '\n'.join(self.append_log)
        self.file_utils.append_to_file(self.status_path, self.log_file, content)

    # Function to find the CBSA ID based on latitude and longitude
    def find_cbsa(self, latitude, longitude):
        # CBSAFP and CBSAID are generally the same, both representing the identifier for a CBSA.
        # CSAFP represents a broader Combined Statistical Area that may contain multiple CBSAs.
        # GEOID is a more general geographic code that can be used for many types of regions (not just CBSAs).

        cbsa_data = self.read_from_json_file(self.cbsa_path_json)
        try:
            for cbsa in cbsa_data:
                if (cbsa['min_lat'] <= latitude <= cbsa['max_lat'] and
                        cbsa['min_lon'] <= longitude <= cbsa['max_lon']):
                    return cbsa
        except Exception as e:
            logging.error(
                f"[{self.get_service_name()}] Error: Unable to get CBSA")

        return None

    def get_remaining_time_sec(self):
        if self.running_in_lambda:
            return self.context.get_remaining_time_in_millis() / 1000
        return 120
