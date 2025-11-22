import csv
import json
import logging
import os

from common.BaseJsonToCsv import BaseJsonToCsv
from common.ParserName import ParserName
from common.ActionName import ActionName


class ImtiazJsonToCsv(BaseJsonToCsv):
    def __init__(self, event, context):
        parser_path = str(os.path.dirname(__file__))
        super().__init__(event, parser_path, context)
        
        # IMPORTANT: Override input path to use post-menu instead of menu
        if not self.running_in_lambda:
            self.input_file_path = "post-menu"
            
        if self.running_in_lambda:
            self.input_file_path = self.input_file_path.replace("menu", "post-menu")

    def get_service_name(self):
        return ParserName.imtiaz.name

    # def parse_menu_csv(self):
    #     """
    #     Override base class method to fix file path issue
    #     """
    #     all_files = self.file_utils.list(self.input_file_path + "/")
    #     total_records = len(all_files)
    #     self.offset_end = total_records + 1
    #     row_parsed = 0
    #     all_parsed = False
    #     temp_file_name = f"{self.get_service_name()}_prices_{self.offset}.csv"
    #     key = os.path.join(self.local_data_path, self.output_file_path, temp_file_name)

    #     if self.running_in_lambda:
    #         self.delete_all_contents(self.local_data_path)

    #     fieldnames = self.csv_headers()
    #     try:
    #         os.remove(key)
    #     except OSError:
    #         pass
    #     os.makedirs(os.path.dirname(key), exist_ok=True)

    #     logging.info(f"[{self.get_service_name()}] Starting CSV generation for {total_records} files from {self.input_file_path}")
    #     logging.info(f"[{self.get_service_name()}] Output CSV: {temp_file_name}")

    #     with open(key, 'a', newline='', encoding='utf-8') as csv_out_file:
    #         writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames)
    #         writer.writeheader()

    #         for f_name in all_files:
    #             if row_parsed >= self.offset:
    #                 if self.running_in_lambda:
    #                     # In Lambda, download from S3
    #                     self.file_utils.download_object(
    #                         os.path.join(self.input_file_path, f_name),
    #                         os.path.join(self.local_data_path, self.input_file_path, f_name)
    #                     )
                    
    #                 try:
    #                     # FIXED PATH: Include input_file_path in the path
    #                     file_path = os.path.join(self.local_data_path, self.input_file_path, f_name)
                        
    #                     logging.info(f"[{self.get_service_name()}] Processing file {row_parsed + 1}/{total_records}: {f_name}")
                        
    #                     item_details_json = self.read_from_json_file(file_path)
    #                     item_id = f_name.split(".")[0]
                        
    #                     if item_details_json is not None:
    #                         self.write_menu_to_csv(item_details_json, item_id, writer)
    #                     else:
    #                         logging.error(f'[{self.get_service_name()}] Data was not inputted for {item_id}')
    #                 except Exception as e:
    #                     logging.error(f'[{self.get_service_name()}] Data was not inputted for {f_name} because of Error: {e}')
    #                     import traceback
    #                     logging.error(traceback.format_exc())
                        
    #             row_parsed += 1

    #             if row_parsed >= self.offset + self.page_size:
    #                 logging.info(f"[{self.get_service_name()}] Menu parsed: {row_parsed}/{total_records}")
    #                 break

    #             if self.get_remaining_time_sec() < 70:
    #                 logging.info(f"[{self.get_service_name()}] Function is suspended because of possible timeout: {row_parsed}/{total_records}")
    #                 break

    #     if row_parsed >= total_records:
    #         logging.info(f"[{self.get_service_name()}] All menu items are parsed, Total record: {total_records}")
    #         all_parsed = True

    #     # Upload final CSV to S3
    #     if self.running_in_lambda:
    #         print("Uploading to s3: " + str(os.path.join(self.output_file_path, temp_file_name)))
    #         self.file_utils.upload_object(key, os.path.join(self.output_file_path, temp_file_name))

    #         if self.address_cache_updated:
    #             local_path = os.path.join(self.local_data_path, self.address_file_name)
    #             s3_cache_path = os.path.join(self.address_file_path, self.address_file_name)
    #             logging.info(f"[{self.get_service_name()}] Uploading to S3: source:{local_path}, destination:{s3_cache_path}")
    #             self.file_utils.upload_object(local_path, s3_cache_path)

    #     percentage = str(round(row_parsed / total_records * 100, 2)) + "%"

    #     if all_parsed:
    #         return {
    #             "parser": self.get_service_name(),
    #             "action": "None",
    #             "use_proxy": False,
    #             "page_size": self.page_size,
    #             "offset": 0,
    #             "has_more": False,
    #             "offset_end": self.offset_end,
    #             "completed": percentage,
    #             "version": self.version,
    #         }
    #     else:
    #         return {
    #             "parser": self.get_service_name(),
    #             "action": ActionName.MAKE_CSV.value,
    #             "use_proxy": False,
    #             "page_size": self.page_size,
    #             "offset": row_parsed,
    #             "has_more": True,
    #             "offset_end": self.offset_end,
    #             "completed": percentage,
    #             "version": self.version,
    #         }

    def write_menu_to_csv(self, api_response, store_id, writer=None):
        """
        Parse Imtiaz menu JSON and write items to CSV
        
        Expected structure (from post-menu):
        {
            "store": {...},
            "menu_detail": {
                "status": 200,
                "msg": "success",
                "data": [products array]
            }
        }
        """
        try:
            # Extract store and menu data
            store = api_response.get('store', {})
            menu_detail = api_response.get('menu_detail', {})
            
            if not menu_detail or not store:
                logging.warning(f"[{self.get_service_name()}] Invalid menu structure for store_id: {store_id}")
                return
            
            # Get products array from menu_detail
            products = menu_detail.get('data', [])
            
            if not products:
                logging.warning(f"[{self.get_service_name()}] No products found for store_id: {store_id}")
                return
            
            logging.info(f"[{self.get_service_name()}] Processing {len(products)} products for store_id: {store_id} ({store.get('area_name', 'N/A')})")
            
            # Process each product
            total_items = 0
            skipped_items = 0
            
            for product in products:
                try:
                    # Extract product information
                    product_id = product.get('id', 'N/A')
                    menu_id = str(product_id)
                    menu_name = product.get('name', 'N/A')
                    menu_description = product.get('desc', '')
                    
                    # Handle price conversion safely
                    try:
                        price_str = str(product.get('price', '0'))
                        price = float(price_str.replace(',', ''))
                    except (ValueError, TypeError):
                        price = 0.0
                    
                    # Use sub-section as parent category (we added this metadata in post-menu)
                    sub_section_name = product.get('sub_section_name', 'N/A')
                    sub_section_id = str(product.get('sub_section_id', 'N/A'))
                    image_url = product.get('img_url', 'N/A')
                    # Generate CSV row using base class method
                    row = self.gen_csv_row(
                        menu_name=menu_name,
                        menu_description=menu_description,
                        price=price,
                        menu_id=menu_id,
                        menu_parent_id=sub_section_id,
                        menu_parent_name=sub_section_name,
                        store=store,
                        image_url=image_url
                    )
                    
                    # Write row to CSV
                    if writer:
                        writer.writerow(row)
                        total_items += 1
                
                except Exception as e:
                    logging.error(f"[{self.get_service_name()}] Error processing product {product.get('id', 'unknown')}: {str(e)}")
                    skipped_items += 1
                    continue
            
            if total_items > 0:
                logging.info(f"[{self.get_service_name()}] Successfully wrote {total_items} items for store_id: {store_id} (skipped: {skipped_items})")
            else:
                logging.warning(f"[{self.get_service_name()}] No products written for store_id: {store_id}")
        
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error writing menu to CSV for store_id {store_id}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())