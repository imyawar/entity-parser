import csv
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.BaseJsonToCsv import BaseJsonToCsv
from common.ActionName import ActionName


class MetroJsonToCsv(BaseJsonToCsv):
    
    def __init__(self, event, context):
        parser_path = os.path.dirname(os.path.abspath(__file__))
        super().__init__(event, parser_path, context)
        
        if not self.running_in_lambda:
            self.input_file_path = "post-menu"
    
    def get_service_name(self):
        return "metro"
    
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
                        if self.running_in_lambda:
                            file_path = os.path.join(self.local_data_path, f_name)
                        else:
                            if os.path.sep in f_name or '/' in f_name:
                                file_path = os.path.join(self.local_data_path, f_name)
                            else:
                                file_path = os.path.join(self.local_data_path, self.input_file_path, f_name)
                        
                        item_details_json = self.read_from_json_file(file_path)
                        item_id = f_name.split(".")[0] if '/' not in f_name else f_name.split("/")[-1].split(".")[0]
                        
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
                    break

        if row_parsed >= total_records:
            logging.info(f"[{self.get_service_name()}] All menu items are parsed, Total record: {total_records}")
            all_parsed = True

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
    
    def write_menu_to_csv(self, api_response, store_id, writer=None):
        try:
            store_detail = api_response.get('store', {})
            menu_details = api_response.get('menu_detail', {})
            
            products = menu_details.get('products', [])
            
            logging.info(f"[{self.get_service_name()}] Processing {len(products)} products for store {store_id}")
            
            for product in products:
                try:
                    product_id = product.get('id', 'N/A')
                    product_name = product.get('product_name', 'N/A')
                    description = product.get('description', 'N/A')
                    
                    price = product.get('price', 0)
                    
                    tier3_id = product.get('tier3Id', 'N/A')
                    tier3_name = product.get('tier3Name', 'N/A')
                    fetch_category = product.get('fetch_category', tier3_name)
                    
                    image_url = product.get('url', 'N/A')
                    
                    row = self.gen_csv_row(
                        menu_name=product_name,
                        menu_description=description,
                        price=price,
                        menu_id=product_id,
                        menu_parent_id=tier3_id,
                        menu_parent_name=fetch_category,
                        store=store_detail,
                        image_url=image_url
                    )
                    
                    if writer:
                        writer.writerow(row)
                    
                except Exception as e:
                    logging.error(f"[{self.get_service_name()}] Error processing product {product.get('id', 'unknown')}: {str(e)}")
                    continue
            
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error in write_menu_to_csv for store {store_id}: {str(e)}")


def lambda_handler(event, context):
    csv_generator = MetroJsonToCsv(event, context)
    return csv_generator.parse_menu_csv()


if __name__ == "__main__":
    event = {
        "use_proxy": False,
        "page_size": 100,
        "offset": 0
    }
    
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000
    
    result = lambda_handler(event, MockContext())
    print(f"CSV generation result: {result}")