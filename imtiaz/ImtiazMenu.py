import json
import logging
import os
from time import sleep
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.BaseMenu import BaseMenu
from common.ParserName import ParserName


class ImtiazMenu(BaseMenu):
    def __init__(self, event, context):
        super().__init__(event, str(os.path.dirname(__file__)), context)
        self.session = self.__create_session()
        self.request_count = 0
        self.max_retries = 2
        self.base_url = "https://shop.imtiaz.com.pk"

    def get_service_name(self):
        return ParserName.imtiaz.name

    def __create_session(self):
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def __get_headers(self):
        """Get headers for API requests"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'app-name': 'imtiazsuperstore',
            'rest-id': '55126',
            'Referer': 'https://shop.imtiaz.com.pk/',
            'Cache-Control': 'no-cache'
        }
        return headers

    def __make_api_request(self, endpoint, params, retry_count=0):
        """Make API request with error handling"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            self.request_count += 1
            if self.request_count > 1:
                sleep(1)
            
            response = self.session.get(
                url,
                headers=self.__get_headers(),
                params=params,
                timeout=15
            )
            
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    if json_response.get('status') == 200:
                        return json_response
                    else:
                        if retry_count < self.max_retries:
                            sleep(2)
                            return self.__make_api_request(endpoint, params, retry_count + 1)
                        return None
                except json.JSONDecodeError:
                    return None
            else:
                if retry_count < self.max_retries:
                    sleep(2)
                    return self.__make_api_request(endpoint, params, retry_count + 1)
                return None
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Request failed: {str(e)}")
            return None

    def get_store_id(self, params):
        return params.get('rest_brId', 'N/A')

    def parse_location_for_menu(self, item_details_json, filename):
        """Extract necessary information from location JSON"""
        return item_details_json

    def gen_request(self, store_data):
        """
        Fetch ONLY the menu structure (categories/sections/sub-sections)
        NOT the actual products - that will be done in post-menu phase
        """
        rest_brId = store_data.get('rest_brId')
        
        if not rest_brId:
            logging.error(f"[{self.get_service_name()}] Missing rest_brId in store_data")
            return None, None
        
        logging.info(f"[{self.get_service_name()}] Fetching menu structure for rest_brId: {rest_brId}")
        
        # Fetch only the menu structure
        menu_structure = self.__fetch_menu_structure(rest_brId)
        
        if not menu_structure:
            logging.error(f"[{self.get_service_name()}] Failed to fetch menu structure for rest_brId: {rest_brId}")
            return rest_brId, None
        
        # Create response with structure only (no products yet)
        response_data = {
            "status": 200,
            "msg": "success",
            "structure": menu_structure  # Just the structure
        }
        
        logging.info(f"[{self.get_service_name()}] ✓ Successfully fetched menu structure with {len(menu_structure.get('sections', []))} sections")
        return rest_brId, self.__create_response_object(response_data)

    def __fetch_menu_structure(self, rest_brId):
        """
        Fetch only the menu/section/sub-section hierarchy
        This is fast and lightweight
        """
        # Step 1: Get menu sections
        menu_sections = self.__fetch_menu_sections(rest_brId)
        if not menu_sections:
            return None
        
        all_sections = []
        
        for menu_section in menu_sections:
            menu_id = menu_section.get('id')
            menu_name = menu_section.get('name', 'Unknown')
            
            logging.info(f"[{self.get_service_name()}] Processing menu: {menu_name}")
            
            sections = menu_section.get('section', [])
            
            for section in sections:
                section_id = section.get('id')
                section_name = section.get('name', 'Unknown')
                
                # Fetch sub-sections
                sub_sections_data = self.__fetch_sub_sections(rest_brId, section_id)
                
                if sub_sections_data:
                    for sub_section_data in sub_sections_data:
                        dish_sub_sections = sub_section_data.get('dish_sub_sections', [])
                        
                        for dish_sub_section in dish_sub_sections:
                            sub_section_id = dish_sub_section.get('id')
                            sub_section_name = dish_sub_section.get('name', 'Unknown')
                            
                            # Store the structure info (NO PRODUCTS YET)
                            all_sections.append({
                                'menu_id': menu_id,
                                'menu_name': menu_name,
                                'section_id': section_id,
                                'section_name': section_name,
                                'sub_section_id': sub_section_id,
                                'sub_section_name': sub_section_name,
                                'rest_brId': rest_brId
                            })
                
                # sleep(0.1)
        
        logging.info(f"[{self.get_service_name()}] Total sections to process: {len(all_sections)}")
        
        return {
            'rest_brId': rest_brId,
            'sections': all_sections
        }

    def __fetch_menu_sections(self, rest_brId):
        """Fetch menu sections"""
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'delivery_type': '0',
            'source': ''
        }
        
        data = self.__make_api_request('/api/menu-section', params)
        
        if data and data.get('data'):
            return data.get('data', [])
        
        return None

    def __fetch_sub_sections(self, rest_brId, section_id):
        """Fetch sub-sections"""
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'sectionId': str(section_id),
            'delivery_type': '0',
            'source': ''
        }
        
        data = self.__make_api_request('/api/sub-section', params)
        
        if data and data.get('data'):
            return data.get('data', [])
        
        return []

    def __create_response_object(self, data):
        """Create response object"""
        class ResponseObject:
            def __init__(self, data):
                self._data = data
                self.status_code = 200
            
            def json(self):
                return self._data
        
        return ResponseObject(data)