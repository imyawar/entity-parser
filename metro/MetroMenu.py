import json
import logging
import sys
import os
from time import sleep
import urllib.parse


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.BaseMenu import BaseMenu


class MetroMenu(BaseMenu):
    
    def __init__(self, event, context):
        parser_path = os.path.dirname(os.path.abspath(__file__))
        super().__init__(event, parser_path, context)
        self.base_url = "https://admin.metro-online.pk/api/read"
        self.categories_cache = {}
        
    def get_service_name(self):
        return "metro"
    
    def get_store_id(self, params):
        return params.get('store_id', 'unknown')
    
    def parse_location_for_menu(self, item_details_json, filename):
        """
        Extract store information from location JSON
        """
        return {
            'store_id': item_details_json.get('store_id'),
            'api_id': item_details_json.get('api_id'),
            'store_name': item_details_json.get('store_name', 'N/A'),
            'location': item_details_json.get('location', 'N/A'),
            'city': item_details_json.get('city', 'N/A'),
            'state': item_details_json.get('state', 'N/A'),
            'address': item_details_json.get('address', 'N/A'),
            'zipcode': item_details_json.get('zipcode', 'N/A'),
            'latitude': item_details_json.get('latitude', 0.0),
            'longitude': item_details_json.get('longitude', 0.0),
            'phone': item_details_json.get('phone', 'N/A'),
            'is_active': item_details_json.get('is_active', False),
            'default_city': item_details_json.get('default_city', False)
        }
    
    def fetch_categories(self, store_id):
        """
        Fetch all product categories for the store
        Returns categories organized by tier level
        """
        if store_id in self.categories_cache:
            return self.categories_cache[store_id]
        
        try:
            categories_url = f"{self.base_url}/Categories"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://www.metro-online.pk',
                'Referer': 'https://www.metro-online.pk/',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            params = [
                ('filter', 'storeId'),
                ('filterValue', str(store_id))
            ]
            
            query_string = urllib.parse.urlencode(params)
            full_url = f"{categories_url}?{query_string}"
            
            response = self.get_request(full_url, headers, None)
            
            if response and response.status_code == 200:
                data = response.json()
                categories = data.get('data', [])
                
                
                self.categories_cache[store_id] = categories
                
                logging.info(f"[{self.get_service_name()}] Fetched {len(categories)} categories for store {store_id}")
                return categories
            else:
                logging.error(f"[{self.get_service_name()}] Failed to fetch categories for store {store_id}")
                return []
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching categories: {str(e)}")
            return []
    
    def fetch_products_by_category(self, store_id, tier3_id, category_name, offset=0, limit=100):
        """
        Fetch products for a specific category (tier3)
        Uses pagination with offset and limit
        """
        try:
            products_url = f"{self.base_url}/Products"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://www.metro-online.pk',
                'Referer': 'https://www.metro-online.pk/',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            filter_params = [
                ('type', 'Products_nd_associated_Brands'),
                ('offset', str(offset)),
                ('limit', str(limit)),
                ('filter', 'tier3Id'),
                ('filterValue', str(tier3_id)),
                ('order', 'product_scoring__DESC'),
                ('filter', 'active'),
                ('filterValue', 'true'),
                ('filter', '!url'),
                ('filterValue', '!null'),
                ('filter', 'storeId'),
                ('filterValue', str(store_id)),
                ('filter', 'Op.available_stock'),
                ('filterValue', 'Op.gt__0')
            ]
            
            query_string = urllib.parse.urlencode(filter_params)
            full_url = f"{products_url}?{query_string}"
            
            response = self.get_request(full_url, headers, None)
            
            if response and response.status_code == 200:
                data = response.json()
                products = data.get('data', [])
                
                # Adding category name to each product
                for product in products:
                    product['fetch_category'] = category_name
                
                return products
            else:
                logging.error(f"[{self.get_service_name()}] Failed to fetch products for category {category_name} (tier3_id: {tier3_id})")
                return []
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching products for category {category_name}: {str(e)}")
            return []
    
    def gen_request(self, store_data):
        """
        Main function to fetch menu (products) for a store
        This is called by BaseMenu.gen_menu() for each store
        """
        store_id = store_data.get('store_id')
        store_name = store_data.get('store_name', 'Unknown')
        
        logging.info(f"[{self.get_service_name()}] Fetching menu for store: {store_name} (ID: {store_id})")
        
        try:
            # Step 1: Fetching all categories for this store
            all_categories = self.fetch_categories(store_id)
            
            if not all_categories:
                logging.warning(f"[{self.get_service_name()}] No categories found for store {store_id}")
                return store_id, None
            
            # Step 2: Build category hierarchy using parentId
            # Tier 1: parentId is None
            tier1_categories = [cat for cat in all_categories if cat.get('parentId') is None]
            tier1_ids = [cat['id'] for cat in tier1_categories]
            
            # Tier 2: parentId is in tier1_ids
            tier2_categories = [cat for cat in all_categories if cat.get('parentId') in tier1_ids]
            tier2_ids = [cat['id'] for cat in tier2_categories]
            
            # Tier 3: parentId is in tier2_ids (these are the product categories)
            tier3_categories = [cat for cat in all_categories if cat.get('parentId') in tier2_ids]
            
            logging.info(f"[{self.get_service_name()}] Category hierarchy - Tier1: {len(tier1_categories)}, Tier2: {len(tier2_categories)}, Tier3: {len(tier3_categories)}")
            
            if not tier3_categories:
                logging.warning(f"[{self.get_service_name()}] No tier3 categories found for store {store_id}")
                return store_id, None
            
            all_products = []
            
            # Step 3: For each tier3 category, fetch products with pagination
            for idx, category in enumerate(tier3_categories, 1):
                category_id = category.get('id')
                category_name = category.get('category_name', 'Unknown')
                
                logging.info(f"[{self.get_service_name()}] [{idx}/{len(tier3_categories)}] Fetching products for category: {category_name} (id: {category_id})")
                
             
                offset = 0
                limit = 100
                category_total = 0
                
                while True:
                    products = self.fetch_products_by_category(
                        store_id, 
                        category_id,  
                        category_name,
                        offset,
                        limit
                    )
                    
                    if not products:
                        break
                    
                    all_products.extend(products)
                    category_total += len(products)
                    
                    
                    if len(products) < limit:
                        break
                    
                    offset += limit
                    sleep(0.5)  
                
                if category_total > 0:
                    logging.info(f"[{self.get_service_name()}]   → Fetched {category_total} products from {category_name}")
                
                
                if idx < len(tier3_categories):
                    sleep(1)
            
            logging.info(f"[{self.get_service_name()}] Total products fetched for store {store_id}: {len(all_products)}")
            
            
            menu_response = {
                'products': all_products,
                'total_count': len(all_products),
                'categories': tier3_categories,
                'store_id': store_id
            }
            
            
            class MockResponse:
                def __init__(self, data):
                    self._data = data
                
                def json(self):
                    return self._data
            
            return store_id, MockResponse(menu_response)
            
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error in gen_request for store {store_id}: {str(e)}")
            return store_id, None


def lambda_handler(event, context):
    """AWS Lambda handler"""
    menu_scraper = MetroMenu(event, context)
    return menu_scraper.gen_menu()


if __name__ == "__main__":
    """For local testing"""
    event = {
        "use_proxy": False,
        "page_size": 1,
        "offset": 0,
        "force_fetch": False,
        "goto_next_step": True
    }
    
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes
    
    result = lambda_handler(event, MockContext())
    print(f"Menu scraping result: {result}")