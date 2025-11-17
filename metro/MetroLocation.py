import json
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.BaseLocation import BaseLocation


class MetroLocation(BaseLocation):
    
    def __init__(self, event, context):
        parser_path = os.path.dirname(os.path.abspath(__file__))
        super().__init__(event, parser_path, context)
        self.base_url = "https://admin.metro-online.pk/api/read"
        
    def get_service_name(self):
        return "metro"
    
    def get_identifier_id(self, params):
        """For metro, we use 'all' as identifier since one API returns everything"""
        return params.get('message', 'all')
    
    def url_page_size(self):
        """Metro doesn't need pagination for locations"""
        return 1
    
    def calculate_center_point(self, geometry):
        """
        Calculate approximate center point from geometry polygon
        Returns (latitude, longitude)
        """
        if not geometry or not isinstance(geometry, list) or len(geometry) == 0:
            return 0.0, 0.0
        
        try:
            
            polygon = geometry[0]
            
            if not polygon or len(polygon) == 0:
                return 0.0, 0.0
            
           
            total_x = 0.0
            total_y = 0.0
            count = len(polygon)
            
            for point in polygon:
                total_x += point.get('x', 0.0)
                total_y += point.get('y', 0.0)
            
            # x is longitude, y is latitude
            avg_longitude = total_x / count
            avg_latitude = total_y / count
            
            return avg_latitude, avg_longitude
            
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error calculating center point: {str(e)}")
            return 0.0, 0.0
    
    def fetch_one_page(self, row, parent_id, size, offset):
        """
        Fetch all Metro stores in one API call
        Metro has multiple stores across Pakistan
        """
        try:
            # Fetch all stores
            stores_url = f"{self.base_url}/Stores"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://www.metro-online.pk',
                'Referer': 'https://www.metro-online.pk/',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = self.get_request(stores_url, headers, None)
            
            if response and response.status_code == 200:
                data = response.json()
                stores = data.get('data', [])
                
                logging.info(f"[{self.get_service_name()}] Found {len(stores)} Metro stores")
                
                
                for store in stores:
                    store_id = store.get('id')
                    metro_store_id = store.get('metro_store_id', store_id)
                    city_name = store.get('city_name', 'N/A')
                    location_name = store.get('location', 'N/A')
                    
                    
                    geometry = store.get('geometry', [])
                    latitude, longitude = self.calculate_center_point(geometry)
                    
                    
                    address_parts = []
                    if location_name and location_name != 'N/A':
                        address_parts.append(location_name)
                    if city_name and city_name != 'N/A':
                        address_parts.append(city_name)
                    
                    full_address = ', '.join(address_parts) if address_parts else 'N/A'
                    
                    
                    state = 'Punjab'  # Default to Punjab as most stores are there
                    if city_name:
                        city_lower = city_name.lower()
                        if city_lower in ['karachi', 'hyderabad']:
                            state = 'Sindh'
                        elif city_lower in ['peshawar']:
                            state = 'Khyber Pakhtunkhwa'
                        elif city_lower in ['quetta']:
                            state = 'Balochistan'
                        elif city_lower in ['islamabad', 'rawalpindi', 'islamabad-rawalpindi']:
                            state = 'Islamabad Capital Territory'
                    
                   
                    location_data = {
                        'store_id': metro_store_id,  
                        'store_name': f"Metro {location_name}",  
                        'location': location_name,
                        'city': city_name,
                        'state': state,
                        'address': full_address,
                        'zipcode': 'N/A',  
                        'latitude': latitude,
                        'longitude': longitude,
                        'phone': 'N/A', 
                        'is_active': True,  
                        'default_city': store.get('default_city', False),
                        'has_geometry': bool(geometry and len(geometry) > 0),
                        'geometry': geometry if geometry else [],  
                        'city_polygon': store.get('city_polygon') if store.get('city_polygon') else [],  
                        'next_day_polygon': store.get('next_day_polygon') if store.get('next_day_polygon') else []
                    }
                    
                   
                    filename = f"{metro_store_id}.json"
                    self.file_utils.write_file(
                        self.output_file_path,
                        filename,
                        json.dumps(location_data, indent=2)
                    )
                    
                    logging.info(f"[{self.get_service_name()}] Saved location: {location_name}, {city_name} (Store ID: {metro_store_id}, Lat: {latitude:.6f}, Lng: {longitude:.6f})")
                    self.append_to_log(f"{metro_store_id},{location_name},{city_name},success")
                
                return len(stores)  
            else:
                logging.error(f"[{self.get_service_name()}] Failed to fetch stores: Status {response.status_code if response else 'No response'}")
                self.append_to_log(f"all,fetch_stores,failure")
                return 0
                
        except Exception as e:
            logging.error(f"[{self.get_service_name()}] Error fetching locations: {str(e)}")
            self.append_to_log(f"all,fetch_stores,error,{str(e)}")
            return 0


def lambda_handler(event, context):
    """AWS Lambda handler"""
    location_scraper = MetroLocation(event, context)
    return location_scraper.gen_location()


if __name__ == "__main__":
    """For local testing"""
    event = {
        "use_proxy": False,
        "page_size": 1,
        "offset": 0
    }
    
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes
    
    result = lambda_handler(event, MockContext())
    print(f"Location scraping result: {result}")