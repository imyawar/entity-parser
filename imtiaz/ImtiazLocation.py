import json
import logging
import os

from common.BaseLocation import BaseLocation
from common.ParserName import ParserName


class ImtiazLocation(BaseLocation):
    def __init__(self, event, context):
        super().__init__(event, str(os.path.dirname(__file__)), context)

    def get_service_name(self):
        return ParserName.imtiaz.name

    def __get_headers(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'Accept': 'application/json, text/plain, */*',
            'app-name': 'imtiazsuperstore',
            'rest-id': '55126',
            'Referer': 'https://shop.imtiaz.com.pk/'
        }
        return headers

    def __get_request(self, params):
        """
        Fetch all Imtiaz locations from geofence API
        """
        url = "https://shop.imtiaz.com.pk/api/geofence"
        query_params = {'restId': '55126'}
        
        return self.get_request(url, self.__get_headers(), query_params, False)

    def gen_location_preprocessor(self):
        """
        Optional preprocessing - not needed for Imtiaz
        """
        pass

    def get_identifier_id(self, params):
        """
        Returns the identifier from CSV row
        For case 1 (all locations), this returns the id from CSV
        """
        return params['id']

    def fetch_one_page(self, row, parent_id, size, offset):
        """
        Main function that fetches and parses locations
        """
        response = self.__get_request(row)
        
        if response:
            # Log successful API call
            content = f"url,{parent_id},{offset},success"
            self.append_to_log(content)
            logging.info(f"[{self.get_service_name()}] Received response for identity:{parent_id}")
            
            # Parse and save all locations
            return self.parse_response(response.json())
        else:
            # Log failed API call
            content = f"url,{parent_id},{offset},failure"
            self.append_to_log(content)
            logging.error(f"[{self.get_service_name()}] Error: Unable to get response, id:{parent_id}")
        
        return 0

    def parse_response(self, api_response):
        """
        Parse the geofence API response and save each geofence as separate JSON
        """
        if api_response.get('status') != 200:
            logging.error(f"[{self.get_service_name()}] API returned error status: {api_response.get('status')}")
            return 0
        
        cities = api_response.get('data', {}).get('cities', [])
        total_geofences = 0
        
        for city in cities:
            city_name = city.get('name', 'Unknown')
            geofences = city.get('geofences', [])
            
            logging.info(f"[{self.get_service_name()}] Processing city: {city_name}, geofences: {len(geofences)}")
            
            for geofence in geofences:
                geofence_id = geofence.get('geofence_id')
                
                if not geofence_id:
                    logging.warning(f"[{self.get_service_name()}] Skipping geofence without geofence_id in city: {city_name}")
                    continue
                
                # Create location data structure
                location_data = {
                    'store_id': str(geofence_id),
                    'geofence_id': geofence_id,
                    'rest_brId': geofence.get('rest_brId'),
                    'area_name': geofence.get('area_name', 'N/A'),
                    'city': city_name,
                    'latitude': geofence.get('lat', '0.0'),
                    'longitude': geofence.get('lng', '0.0'),
                    'address': geofence.get('area_name', 'N/A'),  # Using area_name as address
                    'state': 'Sindh' if city_name == 'Karachi' else 'N/A',  # Can be enhanced
                    'zipcode': 'N/A',
                    'geofence': geofence.get('geoFence', 'N/A'),
                    'min_order': geofence.get('min_order', 0),
                    'delivery_charges': geofence.get('delivery_charges', 0),
                    'max_delivery_time': geofence.get('max_delivery_time', 0),
                    'start_time': geofence.get('start_time', 'N/A'),
                    'end_time': geofence.get('end_time', 'N/A'),
                    'formatted_start_time': geofence.get('formatted_start_time', 'N/A'),
                    'formatted_end_time': geofence.get('formatted_end_time', 'N/A')
                }
                
                # Create filename using geofence_id (unique identifier)
                j_filename = f"{geofence_id}.json"
                
                # Check if file already exists to avoid duplicate work
                if not self.file_utils.file_exists(self.output_file_path, j_filename):
                    logging.info(f"[{self.get_service_name()}] Saving geofence file: {j_filename} (rest_brId: {geofence.get('rest_brId')}, area: {geofence.get('area_name')})")
                    self.file_utils.write_file(self.output_file_path, j_filename, json.dumps(location_data))
                    total_geofences += 1
                else:
                    content = f"file,{j_filename},found,success"
                    self.append_to_log(content)
                    logging.info(f"[{self.get_service_name()}] Geofence file already exists: {j_filename}")
        
        logging.info(f"[{self.get_service_name()}] Total geofences processed: {total_geofences}")
        
        # Return 0 to indicate no more pagination needed (all data in one call)
        return 0