import csv
import logging
import os
import re
import shutil
from datetime import datetime
from io import StringIO

import boto3

from common.ActionName import ActionName
from common.JSONMixin import JSONMixin
from common.LocalUtils import LocalUtils
from common.ParserName import ParserName
from common.S3Utils import S3Utils
from common.Utils import Utils


class BaseJsonToCsv(JSONMixin):
    def __init__(self, event, parser_path, context):
        super().__init__()
        self.running_in_lambda = False
        self.local_data_path = parser_path + "/data"
        self.input_file_path = "menu"
        self.cost_file_path = "post-menu-cost"
        self.output_file_path = "result"
        self.cbsa_path = parser_path + "/../data"
        self.address_file_name  = f"{self.get_service_name()}_address_cache.csv"
        self.context = context
        self.cbsa_path_json = parser_path+"/../data/cbsa_bounding_boxes.json"

        self.cbsa_map = {}
        self.offset = 0
        self.page_size = 500
        self.offset_end = -1
        self.address_cache_updated = False
        self.log_id = ""

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
            self.cost_file_path = f"post-menu-cost/{self.date_str}/{self.get_service_name()}"
            self.output_file_path = f"result/{self.date_str}/{self.get_service_name()}"
            self.address_file_path  = "lat-long-cache"
            self.running_in_lambda = True
            self.file_utils = S3Utils(self.bucket_name)
            # Only /tmp folder is writable in Lambda environment
            self.local_data_path = "/tmp"
            self.local_utils = LocalUtils(self.local_data_path)
        else:
            print("In local")
            self.version = ""
            self.address_file_path  = self.cbsa_path
            self.page_size = 100
            self.local_utils = LocalUtils(self.local_data_path)
            self.file_utils = self.local_utils

        # self.read_cbsa_text()
        self.brand_map = {
            ParserName.rc.name: 1, ParserName.daves.name: 2, ParserName.zaxbys.name: 3,
            ParserName.chickfila.name: 4, ParserName.kfc.name: 5, ParserName.wendy.name: 6,
            ParserName.popeyes.name: 7, ParserName.cjr.name: 8, ParserName.hardees.name: 9,
            ParserName.cpy.name: 10,ParserName.orange.name:11,ParserName.solidcore.name: 12, ParserName.yogasix.name: 13,
            ParserName.imtiaz.name: 14,
            ParserName.metro.name: 15
        }
        self.brand_names_map = {
            ParserName.rc.name: "Raising Cane's",
            ParserName.daves.name: "Dave's Hot Chicken",
            ParserName.zaxbys.name: "Zaxby's",
            ParserName.chickfila.name: "Chick-fil-A",
            ParserName.kfc.name: "KFC",
            ParserName.cpy.name: "CorePower Yoga",
            ParserName.wendy.name: "Wendy's",
            ParserName.popeyes.name: "Popeyes",
            ParserName.cjr.name: "Carl's Jr.",
            ParserName.hardees.name: "Hardee's",
            ParserName.solidcore.name: "[solidcore]",
            ParserName.yogasix.name: "Yoga Six",
            ParserName.orange.name:"Orangetheory Fitness",
            ParserName.imtiaz.name: "Imtiaz Super Market", 
            ParserName.metro.name: "Metro Cash & Carry" 
        }

    def parse_menu_csv(self):
        all_files = self.file_utils.list(self.input_file_path+"/")
        total_records = len(all_files)
        self.offset_end = total_records + 1
        row_parsed = 0
        all_parsed = False
        temp_file_name = f"{self.get_service_name()}_prices_{self.offset}.csv"
        key = os.path.join(self.local_data_path, self.output_file_path, temp_file_name)

        if self.running_in_lambda:
            self.delete_all_contents(self.local_data_path)

        fieldnames = self.csv_headers()
        try:
            os.remove(key)
        except OSError:
            pass
        os.makedirs(os.path.dirname(key), exist_ok=True)

        with open(key, 'a', newline='', encoding='utf-8') as csv_out_file:
            writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames)
            writer.writeheader()

            for f_name in all_files:
                if row_parsed >= self.offset:
                    if self.running_in_lambda:
                        self.file_utils.download_object(f_name,
                                                        os.path.join(self.local_data_path, f_name))
                    try:
                        # remove menu due to error
                        # No such file or directory: '/tmp/menu/post-menu/2025039/hardees/132632.json'
                        item_details_json = self.read_from_json_file(os.path.join(self.local_data_path,f_name))
                        item_id = f_name.split(".")[0]
                        if item_details_json is not None:
                            self.write_menu_to_csv(item_details_json, item_id, writer)
                        else:
                            logging.error('Data was not inputted for %s', item_id)
                    except Exception as e:
                        logging.error(
                            'Data was not inputted for %s because of Error: %s', f_name, e)
                row_parsed += 1

                if row_parsed >= self.offset + self.page_size:
                    logging.info(f"[{self.get_service_name()}] Menu parsed: {row_parsed}/{total_records}")
                    break

                if self.get_remaining_time_sec() < 70:
                    logging.info(
                        f"[{self.get_service_name()}] Function is suspended because of possible timeout: {row_parsed}/{total_records}")
                    # self.page_size = self.offset - row_parsed
                    break

        if row_parsed >= total_records:
            logging.info(f"[{self.get_service_name()}] All menu items are parsed, Total record: {total_records}")
            all_parsed = True

        # Upload final CSV to S3
        if self.running_in_lambda:
            print("Uploading to s3: " + str(os.path.join(self.output_file_path, temp_file_name)))
            self.file_utils.upload_object(key, os.path.join(self.output_file_path, temp_file_name))

            if self.address_cache_updated:
                local_path = os.path.join(self.local_data_path, self.address_file_name)
                s3_cache_path = os.path.join(self.address_file_path, self.address_file_name)
                logging.info(f"[{self.get_service_name()}] Uploading to S3: source:{local_path}, destination:{s3_cache_path}")
                self.file_utils.upload_object(local_path, s3_cache_path)

        percentage = str(round(row_parsed / total_records * 100, 2)) + "%"

        if all_parsed:
            return {
                "parser": self.get_service_name(),
                "action": "None",
                "use_proxy": False,
                "page_size": self.page_size,
                "offset": 0,
                "has_more": False,
                "offset_end": self.offset_end,
                "completed": percentage,
                "version": self.version,
            }
        else:
            return {
                "parser": self.get_service_name(),
                "action": ActionName.MAKE_CSV.value,
                "use_proxy": False,
                "page_size": self.page_size,
                "offset": row_parsed,
                "has_more": True,
                "offset_end": self.offset_end,
                "completed": percentage,
                "version": self.version,
            }

    def get_service_name(self):
        """Override in subclass to provide service name (e.g., 'daves', 'chickfila')."""
        raise NotImplementedError("Subclasses should implement this method")

    def csv_headers(self):
        return ['menu_id', 'menu_parent_id', 'menu_parent_name', 'menu_name', 'menu_name_clean', 'menu_description', 'price',
                'store_id','product_image_url', 'zip_code', 'city', 'state', 'address', 'lat', 'long', 'brand', 'brand_id', 'date',
                'cbsa_id', 'cbsa', 'utcoffset']

    def write_menu_to_csv(self, api_response, store_id, writer=None):
        raise NotImplementedError("Subclasses should implement this method")

    def get_remaining_time_sec(self):
        if self.running_in_lambda:
            return self.context.get_remaining_time_in_millis() / 1000
        return 120

    def delete_all_contents(self, directory):
        # Iterate through all contents of the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
                print(f"Deleted file: {item_path}")
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                print(f"Deleted directory: {item_path}")

    def read_cbsa_text(self):
        list_items_csv = self.local_utils.read_file(self.cbsa_path, "cbsa_db.csv")
        csv_reader = csv.DictReader(StringIO(list_items_csv))
        rows_list = list(csv_reader)
        for i, row in enumerate(rows_list):
            self.cbsa_map[row['geo_id']] = row['name']


    def clean_text_remove_special_characters(self, text):
        if isinstance(text,str):
            cleaned_name = re.sub(r"[™®]", "", text)
            cleaned_name = cleaned_name.strip().lower()
            return cleaned_name

    def clean_text(self, text):
        if isinstance(text, str):  # Check if it's a string to avoid errors
            text = text.strip()  # Remove leading/trailing whitespace
            text = text.replace('\n', ' ').replace('\r', ' ')  # Remove newlines
            text = text.replace(',', ' ')  # Replace commas with spaces (or other suitable character)
            # Optionally, you can remove extra spaces or special characters:
            text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
        return text

    def read_cost_files(self, api_response):
        cost_items = {}
        for file_path in api_response:
            if self.running_in_lambda:
                self.file_utils.download_object(os.path.join(self.cost_file_path, file_path),
                                                os.path.join(self.local_data_path, self.cost_file_path, file_path))
            item_id = file_path.split(".")[0]
            try:
                item_details_json = self.read_from_json_file(os.path.join(self.local_data_path,
                                                                          self.cost_file_path, file_path))

                if item_details_json is not None:
                    cost_items[item_id] = self.find_cost(item_details_json["optiongroups"])

                else:
                    logging.error('Unable to load cost data from file: %s', file_path)
            except Exception as e:
                logging.error(
                    'Unable to parse cost data for:%s, error:%s', item_id, e)

        return cost_items

    def find_cost(self, content1):
        items = []

        def read_cost_data(parent, content, level, oid):
            names = ['Small', 'Medium', 'Large']
            option_group = content
            if option_group is not None:
                for group in option_group:
                    if "options" in group:
                        for option in group["options"]:
                            oid += 1
                            cost = option.get('cost', 0)
                            name = option.get('name', 'N/A')
                            if level == 1:
                                parent = name
                            if (level == 1 or level == 3) and (oid == 1 or name in names):
                                items.append({
                                    "cost": option.get('cost', 0),
                                    "name": option.get('name', 'N/A'),
                                    "item_id": option.get('id', 0),
                                    "parent": parent,
                                })
                            require_deep = (cost == 0 and level == 1) or level > 1
                            if "modifiers" in option and require_deep:
                                read_cost_data(parent, option['modifiers'], level + 1, 0)

        def parse_options():
            a = {}
            for v in items:
                parent = v['parent']
                if parent in a.keys():
                    a[parent] += v['cost']
                else:
                    a[parent] = v['cost']

            return a

        read_cost_data("none", content1, 1, 0)
        return parse_options()

    # Fetches latitude and longitude from Amazon Location Service using the given address.
    def fetch_location_api(self, address):
        """
        Fetches latitude and longitude from Amazon Location Service using the given address.
        """
        client = boto3.client('location')
        try:
            response = client.search_place_index_for_text(
                IndexName='Address2Location',
                Text=address
            )
            if response['Results']:
                coordinates = response['Results'][0]['Place']['Geometry']['Point']
                # Amazon Location API returns coordinates as [longitude, latitude]
                return coordinates[1], coordinates[0]
        except Exception as e:
            print(f"Error fetching lat/long for address '{address}': {e}")
        return None, None

    def get_lat_long(self, address):
        """
        Try address resolution through csv cache file
        If the address file is unavailable or lacks specific entries,
        call the service to fetch the required data,
        update the file with the new entries, and save it back to S3.
        """
        local_path = os.path.join(self.local_data_path, self.address_file_name)
        s3_cache_path = os.path.join(self.address_file_path, self.address_file_name)
        try:
            if self.running_in_lambda:
                if (self.file_utils.file_exists(self.local_data_path, self.address_file_name) == False
                        or self.file_utils.file_exists(self.address_file_path, self.address_file_name)):
                    self.file_utils.download_object(s3_cache_path, local_path)

            # Check the CSV file for existing latitude and longitude
            with open(local_path, mode='r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row['address'] == address:
                        return row['lat'], row['long']
        except FileNotFoundError:
            print(f"CSV file '{local_path}' not found. It will be created.")

        # If not found in the CSV, fetch from Amazon Location API
        print(f"Fetching lat/long for address: {address}")
        lat, long = self.fetch_location_api(address)
        # todo: not thread safe implementation, move this logic from csv to dynamodb
        self.address_cache_updated = True

        if lat and long:
            # Append the new address and its lat/long to the CSV file
            with open(local_path, mode='a', newline='') as csvfile:
                fieldnames = ['address', 'lat', 'long']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header if file is empty
                csvfile.seek(0, 2)  # Move to end of file to check size
                if csvfile.tell() == 0:
                    writer.writeheader()

                writer.writerow({'address': address, 'lat': lat, 'long': long})

        return lat, long

    # Function to find the CBSA ID based on latitude and longitude
    def find_cbsa(self, latitude, longitude):
        # CBSAFP and CBSAID are generally the same, both representing the identifier for a CBSA.
        # CSAFP represents a broader Combined Statistical Area that may contain multiple CBSAs.
        # GEOID is a more general geographic code that can be used for many types of regions (not just CBSAs).

        cbsa_data = self.read_from_json_file(self.cbsa_path_json)
        try:
            for cbsa in cbsa_data:
                if (cbsa['min_lat'] <= float(latitude) <= cbsa['max_lat'] and
                        cbsa['min_lon'] <= float(longitude) <= cbsa['max_lon']):
                    return cbsa
        except Exception as e:
            logging.error(
                f"[{self.get_service_name()}] Error: Unable to get CBSA")

        return None

    def gen_csv_row(self, menu_name, menu_description, price, menu_id, menu_parent_id, menu_parent_name, store,image_url=None):
        cbsa_id = store.get('CBSAFP', 0)
        latitude = store.get('latitude', 0.0)
        longitude = store.get('longitude', 0.0)
        address = store.get('address', 'N/A')
        zipcode = str(store.get('zipcode', 'N/A'))
        city = store.get('city', 'N/A')
        state = store.get('state', 'N/A')

        if latitude == 0.0 and longitude == 0.0 and address != 'N/A':
            combined_address = ", ".join([address, city, state, zipcode])
            latitude, longitude = self.get_lat_long(combined_address.lower())
            cbsa= self.find_cbsa(latitude, longitude)
            if cbsa is not None:
                cbsa_id=cbsa['CBSAFP']

        # cbsa_name = self.cbsa_map.get(cbsa_id, 'N/A')
        cbsa_name = 'N/A'
        return {
            'menu_name': self.clean_text(menu_name),
            'menu_name_clean': self.clean_text_remove_special_characters(menu_name),
            'menu_description': self.clean_text(menu_description),
            'price': price,
            'menu_id': menu_id,
            'menu_parent_id': menu_parent_id,
            'menu_parent_name': self.clean_text(menu_parent_name),
            'store_id': store.get('store_id', 0),
            'product_image_url':image_url,
            'zip_code': zipcode,
            'city': city,
            'state': state,
            'address': address,
            'lat': latitude,
            'long': longitude,
            'brand': self.brand_names_map.get(self.get_service_name()),
            'brand_id': self.brand_map.get(self.get_service_name()),
            'date': store.get('scrape_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            'cbsa_id': cbsa_id,
            'cbsa': cbsa_name,
            'utcoffset': store.get('utcoffset', 0)
        }