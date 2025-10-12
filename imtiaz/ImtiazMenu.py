import json
import logging
import os
from datetime import datetime
from time import sleep

from common.BaseMenu import BaseMenu
from common.ParserName import ParserName


class ImtiazMenu(BaseMenu):
    def __init__(self, event, context):
        super().__init__(event, str(os.path.dirname(__file__)), context)

    def get_service_name(self):
        return ParserName.imtiaz.name

    def __get_headers(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'app-name': 'imtiazsuperstore',
            'rest-id': '55126',
            'Referer': 'https://shop.imtiaz.com.pk/'
        }
        return headers

    def get_store_id(self, params):
        return params.get('rest_brId', 'N/A')

    def parse_location_for_menu(self, item_details_json, filename):
        return item_details_json

    def gen_request(self, store_data):
        """
        Fetch menu items for a specific Imtiaz location
        Returns: (store_id, response_object)
        """
        rest_brId = store_data.get('rest_brId')
        
        if not rest_brId:
            logging.error(f"[{self.get_service_name()}] Missing rest_brId in store_data")
            return None, None
        
        # Follow the website navigation pattern:
        # 1. Get menu sections
        # 2. Get sub-sections for each section  
        # 3. Get products for each sub-section
        all_products = self.__fetch_all_products_website_pattern(rest_brId)
        
        if not all_products:
            logging.error(f"[{self.get_service_name()}] Failed to fetch products for rest_brId: {rest_brId}")
            return rest_brId, None
        
        # Create response structure
        response_data = {
            "status": 200,
            "msg": "success",
            "data": all_products
        }
        
        logging.info(f"[{self.get_service_name()}] Successfully fetched {len(all_products)} products for rest_brId: {rest_brId}")
        return rest_brId, self.__create_response_object(response_data)

    def __fetch_all_products_website_pattern(self, rest_brId):
        """
        Follow the exact website navigation pattern to get products
        """
        # First get menu sections
        menu_sections = self.__fetch_menu_sections(rest_brId)
        if not menu_sections:
            logging.error(f"[{self.get_service_name()}] No menu sections found")
            return None
        
        all_products = []
        
        for menu_section in menu_sections:
            menu_id = menu_section.get('id')
            menu_name = menu_section.get('name', 'Unknown')
            
            # Get sections within this menu
            sections = menu_section.get('section', [])
            
            for section in sections:
                section_id = section.get('id')
                section_name = section.get('name', 'Unknown')
                
                logging.info(f"[{self.get_service_name()}] Processing section: {section_name} (ID: {section_id})")
                
                # Get sub-sections for this section
                sub_sections = self.__fetch_sub_sections(rest_brId, section_id)
                
                for sub_section in sub_sections:
                    sub_section_id = sub_section.get('id')
                    sub_section_name = sub_section.get('name', 'Unknown')
                    
                    logging.info(f"[{self.get_service_name()}] Processing sub-section: {sub_section_name} (ID: {sub_section_id})")
                    
                    # Get products for this sub-section
                    products = self.__fetch_sub_section_products(rest_brId, sub_section_id)
                    
                    # Add category hierarchy info to products
                    for product in products:
                        product['menu_id'] = menu_id
                        product['menu_name'] = menu_name
                        product['section_id'] = section_id
                        product['section_name'] = section_name
                        product['sub_section_id'] = sub_section_id
                        product['sub_section_name'] = sub_section_name
                    
                    all_products.extend(products)
                    
                    # Add delay to avoid rate limiting
                    sleep(0.5)
        
        return all_products

    def __fetch_menu_sections(self, rest_brId):
        """
        Fetch menu sections (categories)
        """
        url = "https://shop.imtiaz.com.pk/api/menu-section"
        
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'delivery_type': '0',
            'source': ''
        }
        
        try:
            response = self.get_request(url, self.__get_headers(), params, verify=False)
            
            if response and response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    menu_sections = data.get('data', [])
                    logging.info(f"[{self.get_service_name()}] Found {len(menu_sections)} menu sections")
                    return menu_sections
                else:
                    logging.error(f"[{self.get_service_name()}] Menu sections API returned status: {data.get('status')}")
            else:
                logging.error(f"[{self.get_service_name()}] Menu sections API failed with status: {response.status_code if response else 'No response'}")
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching menu sections: {str(e)}")
        
        return None

    def __fetch_sub_sections(self, rest_brId, section_id):
        """
        Fetch sub-sections for a given section
        """
        url = "https://shop.imtiaz.com.pk/api/sub-section"
        
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'sectionId': str(section_id),
            'delivery_type': '0',
            'source': ''
        }
        
        try:
            response = self.get_request(url, self.__get_headers(), params, verify=False)
            
            if response and response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    sub_sections_data = data.get('data', [])
                    if sub_sections_data:
                        sub_sections = sub_sections_data[0].get('dish_sub_sections', [])
                        logging.info(f"[{self.get_service_name()}] Found {len(sub_sections)} sub-sections for section {section_id}")
                        return sub_sections
                else:
                    logging.debug(f"[{self.get_service_name()}] Sub-sections API returned status: {data.get('status')} for section {section_id}")
            else:
                logging.debug(f"[{self.get_service_name()}] Sub-sections API failed for section {section_id}")
                
        except Exception as e:
            logging.debug(f"[{self.get_service_name()}] Error fetching sub-sections for section {section_id}: {str(e)}")
        
        return []

    def __fetch_sub_section_products(self, rest_brId, sub_section_id, page_no=1, per_page=100):
        """
        Fetch products for a specific sub-section (this is the working endpoint!)
        """
        url = "https://shop.imtiaz.com.pk/api/items-by-subsection"
        
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'sub_section_id': str(sub_section_id),
            'delivery_type': '0',
            'source': '',
            'brand_name': '',
            'min_price': '0',
            'max_price': '',
            'sort_by': '',
            'sort': '',
            'page_no': str(page_no),
            'per_page': str(per_page),
            'start': '0',
            'limit': str(per_page)
        }
        
        try:
            response = self.get_request(url, self.__get_headers(), params, verify=False)
            
            if response and response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    products = data.get('data', [])
                    if products:
                        logging.info(f"[{self.get_service_name()}] Found {len(products)} products in sub-section {sub_section_id}")
                    return products
                else:
                    logging.debug(f"[{self.get_service_name()}] No products in sub-section {sub_section_id}, status: {data.get('status')}")
            else:
                logging.debug(f"[{self.get_service_name()}] Products API failed for sub-section {sub_section_id}")
                
        except Exception as e:
            logging.debug(f"[{self.get_service_name()}] Error fetching products for sub-section {sub_section_id}: {str(e)}")
        
        return []

    def __create_response_object(self, data):
        """
        Create a response object
        """
        class ResponseObject:
            def __init__(self, data):
                self._data = data
                self.status_code = 200
            
            def json(self):
                return self._data
        
        return ResponseObject(data)

    def find_cbsa(self, latitude, longitude):
        """
        Override to handle CBSA path correctly
        """
        try:
            parser_dir = os.path.dirname(__file__)
            cbsa_path = os.path.join(parser_dir, "..", "..", "data", "cbsa_bounding_boxes.json")
            
            if os.path.exists(cbsa_path):
                cbsa_data = self.read_from_json_file(cbsa_path)
                for cbsa in cbsa_data:
                    if (cbsa['min_lat'] <= float(latitude) <= cbsa['max_lat'] and
                            cbsa['min_lon'] <= float(longitude) <= cbsa['max_lon']):
                        return cbsa
        except Exception as e:
            logging.debug(f"[{self.get_service_name()}] Error finding CBSA: {str(e)}")
        
        return None