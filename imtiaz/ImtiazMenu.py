import json
import logging
import os
from datetime import datetime
from time import sleep, time
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
        
        # Add detailed logging
        logging.info(f"[{self.get_service_name()}] ImtiazMenu initialized")
        logging.info(f"[{self.get_service_name()}] Input path: {self.input_file_path}")
        logging.info(f"[{self.get_service_name()}] Output path: {self.output_file_path}")

    def get_service_name(self):
        return ParserName.imtiaz.name

    def __create_session(self):
        """
        Create a requests session with retry strategy
        """
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
        """
        Get headers for API requests
        """
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
        """
        Make API request with error handling
        """
        url = f"{self.base_url}{endpoint}"
        
        start_time = time()
        
        try:
            self.request_count += 1
            
            # Rate limiting
            if self.request_count > 1:
                sleep(1)
            
            print(f"\n[DEBUG] Request #{self.request_count}")
            print(f"[DEBUG] URL: {url}")
            print(f"[DEBUG] Params: {params}")
            
            response = self.session.get(
                url,
                headers=self.__get_headers(),
                params=params,
                timeout=15
            )
            
            elapsed = time() - start_time
            print(f"[DEBUG] Response status: {response.status_code} (took {elapsed:.2f}s)")
            
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    api_status = json_response.get('status')
                    
                    print(f"[DEBUG] API status: {api_status}")
                    
                    if api_status == 200:
                        data_size = len(str(json_response.get('data', [])))
                        print(f"[DEBUG] ✓ Success! Data size: {data_size} chars")
                        return json_response
                    else:
                        print(f"[DEBUG] ✗ API error: {json_response.get('msg', 'Unknown error')}")
                        
                        if retry_count < self.max_retries:
                            print(f"[DEBUG] Retrying... ({retry_count + 1}/{self.max_retries})")
                            sleep(2)
                            return self.__make_api_request(endpoint, params, retry_count + 1)
                        return None
                        
                except json.JSONDecodeError as e:
                    print(f"[DEBUG] ✗ JSON parse error: {str(e)}")
                    print(f"[DEBUG] Response text: {response.text[:200]}")
                    return None
                    
            else:
                print(f"[DEBUG] ✗ HTTP error: {response.status_code}")
                if retry_count < self.max_retries:
                    print(f"[DEBUG] Retrying... ({retry_count + 1}/{self.max_retries})")
                    sleep(2)
                    return self.__make_api_request(endpoint, params, retry_count + 1)
                return None
                
        except requests.exceptions.Timeout:
            print(f"[DEBUG] ✗ Request timeout after {time() - start_time:.2f}s")
            if retry_count < self.max_retries:
                print(f"[DEBUG] Retrying... ({retry_count + 1}/{self.max_retries})")
                sleep(2)
                return self.__make_api_request(endpoint, params, retry_count + 1)
            return None
            
        except Exception as e:
            print(f"[DEBUG] ✗ Unexpected error: {type(e).__name__}: {str(e)}")
            return None

    def get_store_id(self, params):
        return params.get('rest_brId', 'N/A')

    def parse_location_for_menu(self, item_details_json, filename):
        """
        Extract necessary information from location JSON
        """
        print(f"\n[DEBUG] parse_location_for_menu called for: {filename}")
        print(f"[DEBUG] rest_brId: {item_details_json.get('rest_brId')}")
        print(f"[DEBUG] area: {item_details_json.get('area_name')}")
        return item_details_json

    def gen_request(self, store_data):
        """
        Fetch menu items for a specific Imtiaz location
        """
        print("\n" + "="*60)
        print(f"[DEBUG] gen_request called")
        print("="*60)
        
        rest_brId = store_data.get('rest_brId')
        
        if not rest_brId:
            print(f"[DEBUG] ✗ Missing rest_brId in store_data!")
            logging.error(f"[{self.get_service_name()}] Missing rest_brId in store_data")
            return None, None
        
        print(f"[DEBUG] rest_brId: {rest_brId}")
        print(f"[DEBUG] area_name: {store_data.get('area_name', 'N/A')}")
        print(f"[DEBUG] city: {store_data.get('city', 'N/A')}")
        
        logging.info(f"[{self.get_service_name()}] Starting menu fetch for rest_brId: {rest_brId}")
        
        # Fetch products
        print(f"[DEBUG] Calling __fetch_all_products_website_flow...")
        all_products = self.__fetch_all_products_website_flow(rest_brId)
        
        if not all_products:
            print(f"[DEBUG] ✗ No products returned!")
            logging.error(f"[{self.get_service_name()}] Failed to fetch products for rest_brId: {rest_brId}")
            return rest_brId, None
        
        print(f"[DEBUG] ✓ Got {len(all_products)} products!")
        
        # Create response
        response_data = {
            "status": 200,
            "msg": "success", 
            "data": all_products
        }
        
        logging.info(f"[{self.get_service_name()}] Successfully fetched {len(all_products)} products")
        return rest_brId, self.__create_response_object(response_data)

    def __fetch_all_products_website_flow(self, rest_brId):
        """
        Follow the website navigation flow
        """
        print(f"\n[DEBUG] __fetch_all_products_website_flow called for rest_brId: {rest_brId}")
        
        # Fetch menu sections
        print(f"[DEBUG] Fetching menu sections...")
        menu_sections = self.__fetch_menu_sections(rest_brId)
        
        if not menu_sections:
            print(f"[DEBUG] ✗ No menu sections returned!")
            logging.error(f"[{self.get_service_name()}] No menu sections found")
            return None
        
        print(f"[DEBUG] ✓ Got {len(menu_sections)} menu sections")
        
        all_products = []
        
        for idx, menu_section in enumerate(menu_sections):
            menu_id = menu_section.get('id')
            menu_name = menu_section.get('name', 'Unknown')
            
            print(f"\n[DEBUG] Processing menu {idx+1}/{len(menu_sections)}: {menu_name}")
            
            sections = menu_section.get('section', [])
            print(f"[DEBUG]   Found {len(sections)} sections")
            
            if not sections:
                continue
            
            for section_idx, section in enumerate(sections):
                section_id = section.get('id')
                section_name = section.get('name', 'Unknown')
                
                print(f"[DEBUG]   Processing section {section_idx+1}/{len(sections)}: {section_name}")
                
                # Fetch sub-sections
                sub_sections_response = self.__fetch_sub_sections(rest_brId, section_id)
                
                if not sub_sections_response:
                    print(f"[DEBUG]     No sub-sections")
                    continue
                
                # Process products
                products = self.__process_sub_sections(
                    rest_brId,
                    sub_sections_response,
                    menu_id,
                    menu_name,
                    section_id,
                    section_name
                )
                
                print(f"[DEBUG]     Got {len(products)} products from section")
                all_products.extend(products)
        
        print(f"\n[DEBUG] Total products collected: {len(all_products)}")
        return all_products if all_products else None

    def __process_sub_sections(self, rest_brId, sub_sections_data, menu_id, menu_name, section_id, section_name):
        """
        Process sub-sections
        """
        products = []
        
        for sub_section_data in sub_sections_data:
            dish_sub_sections = sub_section_data.get('dish_sub_sections', [])
            
            for dish_sub_section in dish_sub_sections:
                sub_section_id = dish_sub_section.get('id')
                sub_section_name = dish_sub_section.get('name', 'Unknown')
                
                print(f"[DEBUG]       Sub-section: {sub_section_name}")
                
                # Fetch products
                sub_products = self.__fetch_all_sub_section_products(rest_brId, sub_section_id)
                
                if sub_products:
                    # Add metadata
                    for product in sub_products:
                        product['menu_id'] = menu_id
                        product['menu_name'] = menu_name
                        product['section_id'] = section_id
                        product['section_name'] = section_name
                        product['sub_section_id'] = sub_section_id
                        product['sub_section_name'] = sub_section_name
                    
                    products.extend(sub_products)
                    print(f"[DEBUG]         Added {len(sub_products)} products")
                
                sleep(1)
        
        return products

    def __fetch_menu_sections(self, rest_brId):
        """
        Fetch menu sections
        """
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
        """
        Fetch sub-sections
        """
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

    def __fetch_all_sub_section_products(self, rest_brId, sub_section_id):
        """
        Fetch all products with pagination
        """
        all_products = []
        page_no = 1
        per_page = 100
        
        while True:
            products = self.__fetch_sub_section_products(rest_brId, sub_section_id, page_no, per_page)
            
            if not products:
                break
            
            all_products.extend(products)
            
            if len(products) < per_page:
                break
            
            page_no += 1
            sleep(0.5)
        
        return all_products

    def __fetch_sub_section_products(self, rest_brId, sub_section_id, page_no=1, per_page=100):
        """
        Fetch products page
        """
        start = (page_no - 1) * per_page
        
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
            'start': str(start),
            'limit': str(per_page)
        }
        
        data = self.__make_api_request('/api/items-by-subsection', params)
        
        if data and data.get('data'):
            return data.get('data', [])
        
        return []

    def __create_response_object(self, data):
        """
        Create response object
        """
        class ResponseObject:
            def __init__(self, data):
                self._data = data
                self.status_code = 200
            
            def json(self):
                return self._data
        
        return ResponseObject(data)