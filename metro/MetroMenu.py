import json
import logging
import sys
import os
import urllib.parse

# Add parent directory to path to import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.BaseMenu import BaseMenu


class MetroMenu(BaseMenu):
    
    def __init__(self, event, context):
        parser_path = os.path.dirname(os.path.abspath(__file__))
        super().__init__(event, parser_path, context)
        self.base_url = "https://admin.metro-online.pk/api/read"
        
    def get_service_name(self):
        return "metro"
    
    def get_store_id(self, params):
        return params.get('store_id')
    
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
                
                logging.info(f"[{self.get_service_name()}] Fetched {len(categories)} categories for store {store_id}")
                return categories
            else:
                logging.error(f"[{self.get_service_name()}] Failed to fetch categories for store {store_id}")
                return []
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching categories: {str(e)}")
            return []
    
    def gen_request(self, store_data):
        """
        Main function to fetch menu (categories only) for a store
        This is called by BaseMenu.gen_menu() for each store
        
        NOTE: We only fetch categories here, not products!
        Products will be fetched in PostMenu to avoid Lambda timeout
        """
        store_id = store_data.get('store_id')
        store_name = store_data.get('store_name', 'Unknown')
        
        logging.info(f"[{self.get_service_name()}] Fetching categories for store: {store_name} (ID: {store_id})")
        
        try:
            # Fetch all categories for this store
            all_categories = self.fetch_categories(store_id)
            
            if not all_categories:
                logging.warning(f"[{self.get_service_name()}] No categories found for store {store_id}")
                return store_id, None
            
            # Build category hierarchy using parentId
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
            
            # Return menu data with categories only (no products yet)
            menu_response = {
                'categories': tier3_categories,
                'all_categories': all_categories,
                'tier1_categories': tier1_categories,
                'tier2_categories': tier2_categories,
                'store_id': store_id,
                'total_categories': len(tier3_categories)
            }
            
            # Create a mock response object with json() method
            # This matches what BaseMenu expects (menu_details.json())
            class MockResponse:
                def __init__(self, data):
                    self._data = data
                
                def json(self):
                    return self._data
            
            logging.info(f"[{self.get_service_name()}] Successfully fetched {len(tier3_categories)} categories for store {store_id}")
            
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