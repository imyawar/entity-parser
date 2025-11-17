import json
import logging
import os
from time import sleep
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.BasePostMenu import BasePostMenu
from common.ParserName import ParserName


class ImtiazPostMenu(BasePostMenu):
    def __init__(self, event, context):
        super().__init__(event, str(os.path.dirname(__file__)), context)
        self.session = self.__create_session()
        self.base_url = "https://shop.imtiaz.com.pk"

    def get_service_name(self):
        return ParserName.imtiaz.name

    def __create_session(self):
        """Create a requests session"""
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def __get_headers(self):
        """Get headers for API requests"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'app-name': 'imtiazsuperstore',
            'rest-id': '55126',
            'Referer': 'https://shop.imtiaz.com.pk/'
        }
        return headers

    def parse_items(self, menu_details, menu_id):
        """
        Parse menu structure and fetch products for each section
        
        menu_details contains the structure from menu phase:
        {
            "status": 200,
            "structure": {
                "rest_brId": 54943,
                "sections": [
                    {
                        "menu_id": ...,
                        "section_id": ...,
                        "sub_section_id": ...,
                        ...
                    }
                ]
            }
        }
        """
        structure = menu_details.get('structure', {})
        sections = structure.get('sections', [])
        rest_brId = structure.get('rest_brId')
        
        if not rest_brId or not sections:
            logging.error(f"[{self.get_service_name()}] Invalid structure for menu_id: {menu_id}")
            self.append_to_log(f"{menu_id},structure,invalid,failure")
            return menu_details, []
        
        logging.info(f"[{self.get_service_name()}] Fetching products for {len(sections)} sections (rest_brId: {rest_brId})")
        self.append_to_log(f"{menu_id},start,sections:{len(sections)},rest_brId:{rest_brId},success")
        
        all_products = []
        cost_files = []
        
        # Fetch products for each section
        for idx, section_info in enumerate(sections):
            sub_section_id = section_info.get('sub_section_id')
            sub_section_name = section_info.get('sub_section_name', 'Unknown')
            
            logging.info(f"[{self.get_service_name()}] [{idx+1}/{len(sections)}] Fetching products for: {sub_section_name}")
            
            # Fetch products for this sub-section
            products = self.__fetch_sub_section_products(rest_brId, sub_section_id)
            
            if products:
                # Add hierarchy metadata to products
                for product in products:
                    product['menu_id'] = section_info.get('menu_id')
                    product['menu_name'] = section_info.get('menu_name')
                    product['section_id'] = section_info.get('section_id')
                    product['section_name'] = section_info.get('section_name')
                    product['sub_section_id'] = sub_section_id
                    product['sub_section_name'] = sub_section_name
                
                all_products.extend(products)
                logging.info(f"[{self.get_service_name()}]   Added {len(products)} products")
                self.append_to_log(f"{menu_id},subsection:{sub_section_id},name:{sub_section_name},products:{len(products)},success")
            else:
                self.append_to_log(f"{menu_id},subsection:{sub_section_id},name:{sub_section_name},products:0,no_data")
            
            # Rate limiting
            sleep(1)
        
        logging.info(f"[{self.get_service_name()}] Total products fetched: {len(all_products)}")
        self.append_to_log(f"{menu_id},complete,total_products:{len(all_products)},success")
        
        # Create updated menu_details with products
        updated_menu_details = {
            "status": 200,
            "msg": "success",
            "data": all_products
        }
        
        return updated_menu_details, cost_files

    def __fetch_sub_section_products(self, rest_brId, sub_section_id):
        """Fetch all products for a sub-section with pagination"""
        all_products = []
        page_no = 1
        per_page = 100
        
        while True:
            products = self.__fetch_products_page(rest_brId, sub_section_id, page_no, per_page)
            
            if not products:
                break
            
            all_products.extend(products)
            
            if len(products) < per_page:
                break
            
            page_no += 1
            sleep(0.5)
        
        return all_products

    def __fetch_products_page(self, rest_brId, sub_section_id, page_no=1, per_page=100):
        """Fetch a single page of products"""
        url = f"{self.base_url}/api/items-by-subsection"
        
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
        
        try:
            response = self.session.get(
                url,
                headers=self.__get_headers(),
                params=params,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    return data.get('data', [])
        
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching products: {str(e)}")
        
        return []

    def gen_request(self, menu_id):
        """Not needed for Imtiaz - we fetch products in parse_items"""
        pass
