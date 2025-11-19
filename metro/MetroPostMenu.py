import json
import logging
import sys
import os
import urllib.parse
from time import sleep

# Add parent directory to path to import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.BasePostMenu import BasePostMenu


class MetroPostMenu(BasePostMenu):
    
    def __init__(self, event, context):
        parser_path = os.path.dirname(os.path.abspath(__file__))
        super().__init__(event, parser_path, context)
        self.base_url = "https://admin.metro-online.pk/api/read"
        
    def get_service_name(self):
        return "metro"
    
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
                
                # Add category name to each product
                for product in products:
                    product['fetch_category'] = category_name
                
                return products
            else:
                logging.error(f"[{self.get_service_name()}] Failed to fetch products for category {category_name} (tier3_id: {tier3_id})")
                return []
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching products for category {category_name}: {str(e)}")
            return []
    
    def parse_items(self, menu_details, menu_id):
        """
        This is called by BasePostMenu.read_menu() for each menu file
        
        menu_details contains the categories from MetroMenu
        We need to fetch products for each category
        
        Returns: (updated_menu_details, file_locations)
        """
        try:
            store_id = menu_details.get('store_id')
            tier3_categories = menu_details.get('categories', [])
            
            if not tier3_categories:
                logging.warning(f"[{self.get_service_name()}] No categories found in menu_id: {menu_id}")
                return menu_details, []
            
            logging.info(f"[{self.get_service_name()}] Processing {len(tier3_categories)} categories for store {store_id}")
            
            all_products = []
            
            # For each tier3 category, fetch products with pagination
            for idx, category in enumerate(tier3_categories, 1):
                category_id = category.get('id')
                category_name = category.get('category_name', 'Unknown')
                
                logging.info(f"[{self.get_service_name()}] [{idx}/{len(tier3_categories)}] Fetching products for category: {category_name} (id: {category_id})")
                
                # Fetch products with pagination
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
                    
                    # If we got less than limit, we've reached the end
                    if len(products) < limit:
                        break
                    
                    offset += limit
                    sleep(0.5)  # Small delay between pagination requests
                
                if category_total > 0:
                    logging.info(f"[{self.get_service_name()}]   → Fetched {category_total} products from {category_name}")
                
                # Delay between categories to be respectful to the server
                if idx < len(tier3_categories):
                    sleep(1)
            
            logging.info(f"[{self.get_service_name()}] Total products fetched for store {store_id}: {len(all_products)}")
            
            # Update menu_details with products
            updated_menu_details = {
                'products': all_products,
                'total_count': len(all_products),
                'categories': tier3_categories,
                'all_categories': menu_details.get('all_categories', []),
                'tier1_categories': menu_details.get('tier1_categories', []),
                'tier2_categories': menu_details.get('tier2_categories', []),
                'store_id': store_id
            }
            
            # No cost files for Metro (return empty list)
            file_locations = []
            
            self.append_to_log(f"{menu_id},products_fetched,{len(all_products)},success")
            
            return updated_menu_details, file_locations
            
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error in parse_items for menu_id {menu_id}: {str(e)}")
            self.append_to_log(f"{menu_id},parse_items,error,{str(e)}")
            return menu_details, []
    
    def gen_request(self, menu_id):
        """
        Not used in Metro's case since we process items through parse_items
        Required by BasePostMenu but we don't need to make additional requests
        """
        pass


def lambda_handler(event, context):
    """AWS Lambda handler"""
    post_menu_scraper = MetroPostMenu(event, context)
    return post_menu_scraper.associate_missing_price()


if __name__ == "__main__":
    """For local testing"""
    event = {
        "use_proxy": False,
        "page_size": 10,
        "offset": 0
    }
    
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes
    
    result = lambda_handler(event, MockContext())
    print(f"Post-menu processing result: {result}")