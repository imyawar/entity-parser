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
            
            # Parse and save branch locations (not individual geofences)
            return self.parse_response(response.json())
        else:
            # Log failed API call
            content = f"url,{parent_id},{offset},failure"
            self.append_to_log(content)
            logging.error(f"[{self.get_service_name()}] Error: Unable to get response, id:{parent_id}")
        
        return 0

    def parse_response(self, api_response):
        """
        Parse the geofence API response and create ONE file per unique rest_brId
        This consolidates all geofences into their parent branches
        """
        if api_response.get('status') != 200:
            logging.error(f"[{self.get_service_name()}] API returned error status: {api_response.get('status')}")
            return 0
        
        cities = api_response.get('data', {}).get('cities', [])
        
        # Group geofences by rest_brId (branch)
        branches = {}
        
        for city in cities:
            city_name = city.get('name', 'Unknown')
            geofences = city.get('geofences', [])
            
            logging.info(f"[{self.get_service_name()}] Processing city: {city_name}, geofences: {len(geofences)}")
            
            for geofence in geofences:
                rest_brId = geofence.get('rest_brId')
                
                if not rest_brId:
                    logging.warning(f"[{self.get_service_name()}] Skipping geofence without rest_brId")
                    continue
                
                # Initialize branch if not exists
                if rest_brId not in branches:
                    branches[rest_brId] = {
                        'store_id': str(rest_brId),
                        'rest_brId': rest_brId,
                        'city': city_name,
                        'state': 'Sindh' if city_name == 'Karachi' else 'N/A',
                        'zipcode': 'N/A',
                        'geofences': [],
                        'total_geofences': 0
                    }
                
                # Add geofence info to branch
                branches[rest_brId]['geofences'].append({
                    'geofence_id': geofence.get('geofence_id'),
                    'area_name': geofence.get('area_name', 'N/A'),
                    'latitude': geofence.get('lat', '0.0'),
                    'longitude': geofence.get('lng', '0.0'),
                    'geofence': geofence.get('geoFence', 'N/A'),
                    'min_order': geofence.get('min_order', 0),
                    'delivery_charges': geofence.get('delivery_charges', 0),
                    'max_delivery_time': geofence.get('max_delivery_time', 0)
                })
                
                branches[rest_brId]['total_geofences'] += 1
        
        # Save one file per branch
        total_branches = 0
        for rest_brId, branch_data in branches.items():
            # Use first geofence for branch-level lat/long (or calculate average)
            first_geofence = branch_data['geofences'][0] if branch_data['geofences'] else {}
            
            # Create branch location data
            location_data = {
                'store_id': str(rest_brId),
                'rest_brId': rest_brId,
                'city': branch_data['city'],
                'state': branch_data['state'],
                'zipcode': branch_data['zipcode'],
                'latitude': first_geofence.get('latitude', '0.0'),
                'longitude': first_geofence.get('longitude', '0.0'),
                'address': f"Branch {rest_brId} - {branch_data['city']}",
                'area_name': f"Branch {rest_brId}",
                'total_geofences': branch_data['total_geofences'],
                'geofences': branch_data['geofences']  # All delivery areas for this branch
            }
            
            # Create filename using rest_brId (branch identifier)
            j_filename = f"{rest_brId}.json"
            
            # Check if file already exists
            if not self.file_utils.file_exists(self.output_file_path, j_filename):
                logging.info(f"[{self.get_service_name()}] Saving branch file: {j_filename} (rest_brId: {rest_brId}, geofences: {branch_data['total_geofences']})")
                self.file_utils.write_file(self.output_file_path, j_filename, json.dumps(location_data))
                total_branches += 1
            else:
                content = f"file,{j_filename},found,success"
                self.append_to_log(content)
                logging.info(f"[{self.get_service_name()}] Branch file already exists: {j_filename}")
        
        logging.info(f"[{self.get_service_name()}] Total branches processed: {total_branches}")
        logging.info(f"[{self.get_service_name()}] Branch IDs: {list(branches.keys())}")
        
        # Return 0 to indicate no more pagination needed
        return 0