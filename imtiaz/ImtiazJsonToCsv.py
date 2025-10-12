import json
import logging
import os

from common.BaseJsonToCsv import BaseJsonToCsv
from common.ParserName import ParserName


class ImtiazJsonToCsv(BaseJsonToCsv):
    def __init__(self, event, context):
        parser_path = str(os.path.dirname(__file__))
        super().__init__(event, parser_path, context)

    def get_service_name(self):
        return ParserName.imtiaz.name

    def write_menu_to_csv(self, api_response, store_id, writer=None):
        """
        Parse Imtiaz menu JSON and write items to CSV
        """
        try:
            store = api_response.get('store', {})
            menu_detail = api_response.get('menu_detail', {})
            
            if not menu_detail or not store:
                logging.warning(f"[{self.get_service_name()}] Invalid menu structure for store_id: {store_id}")
                return
            
            # Get products from menu_detail
            products = menu_detail.get('data', [])
            
            if not products:
                logging.warning(f"[{self.get_service_name()}] No products found for store_id: {store_id}")
                return
            
            # Process each product
            total_items = 0
            for product in products:
                try:
                    menu_id = str(product.get('id', 'N/A'))
                    menu_name = product.get('name', 'N/A')
                    menu_description = product.get('desc', '')
                    
                    # Handle price conversion safely
                    try:
                        price = float(product.get('price', 0))
                    except (ValueError, TypeError):
                        price = 0.0
                    
                    # Use sub-section as parent category
                    sub_section_name = product.get('sub_section_name', 'N/A')
                    sub_section_id = product.get('sub_section_id', 'N/A')
                    
                    # Generate CSV row using base class method
                    row = self.gen_csv_row(
                        menu_name=menu_name,
                        menu_description=menu_description,
                        price=price,
                        menu_id=menu_id,
                        menu_parent_id=sub_section_id,
                        menu_parent_name=sub_section_name,
                        store=store
                    )
                    
                    # Write row to CSV
                    if writer:
                        writer.writerow(row)
                        total_items += 1
                
                except Exception as e:
                    logging.error(f"[{self.get_service_name()}] Error processing product {product.get('id', 'unknown')}: {str(e)}")
                    continue
            
            logging.info(f"[{self.get_service_name()}] Successfully wrote {total_items} items for store_id: {store_id}")
        
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error writing menu to CSV for store_id {store_id}: {str(e)}")