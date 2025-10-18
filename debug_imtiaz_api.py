import requests
import json

# Test the menu-section API with different parameters
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
    'Accept': 'application/json, text/plain, */*',
    'app-name': 'imtiazsuperstore',
    'rest-id': '55126',
    'Referer': 'https://shop.imtiaz.com.pk/'
}

print("="*60)
print("TESTING IMTIAZ API ENDPOINTS")
print("="*60)

# First, let's get all geofences to see which rest_brId values exist
print("\n1. Fetching all geofences...")
response = requests.get(
    'https://shop.imtiaz.com.pk/api/geofence',
    headers=headers,
    params={'restId': '55126'}
)

if response.status_code == 200:
    data = response.json()
    
    # Collect all unique rest_brId values
    rest_br_ids = set()
    geofence_map = {}
    
    for city in data.get('data', {}).get('cities', []):
        city_name = city.get('name')
        for geo in city.get('geofences', []):
            rest_brId = geo.get('rest_brId')
            rest_br_ids.add(rest_brId)
            geofence_map[rest_brId] = {
                'city': city_name,
                'area': geo.get('area_name'),
                'geofence_id': geo.get('geofence_id')
            }
    
    print(f"✓ Total geofences: {len(geofence_map)}")
    print(f"✓ Unique rest_brId values: {len(rest_br_ids)}")
    print(f"\nSample rest_brId values: {sorted(list(rest_br_ids))[:10]}")
    
    # Now test menu-section API with different rest_brId values
    print("\n" + "="*60)
    print("2. Testing menu-section API with different rest_brId values...")
    print("="*60)
    
    test_ids = sorted(list(rest_br_ids))[:5]  # Test first 5 unique IDs
    
    for rest_brId in test_ids:
        info = geofence_map.get(rest_brId, {})
        print(f"\n--- Testing rest_brId: {rest_brId} ---")
        print(f"    City: {info.get('city')}, Area: {info.get('area')}")
        
        # Try with delivery_type=0
        params = {
            'restId': '55126',
            'rest_brId': str(rest_brId),
            'delivery_type': '0',
            'source': ''
        }
        
        try:
            response = requests.get(
                'https://shop.imtiaz.com.pk/api/menu-section',
                headers=headers,
                params=params,
                timeout=10
            )
            
            print(f"    Status: {response.status_code}")
            
            if response.status_code == 200:
                menu_data = response.json()
                if menu_data.get('status') == 200:
                    categories = menu_data.get('data', [])
                    print(f"    ✓ SUCCESS! Found {len(categories)} categories")
                    if len(categories) > 0:
                        print(f"    First category: {categories[0].get('name')}")
                else:
                    print(f"    ✗ API returned error status: {menu_data.get('status')}")
                    print(f"    Message: {menu_data.get('msg')}")
            else:
                print(f"    ✗ HTTP Error: {response.status_code}")
                print(f"    Response: {response.text[:200]}")
                
        except Exception as e:
            print(f"    ✗ Exception: {str(e)}")
    
    # Try without delivery_type parameter
    print("\n" + "="*60)
    print("3. Testing without delivery_type parameter...")
    print("="*60)
    
    rest_brId = test_ids[0]
    info = geofence_map.get(rest_brId, {})
    print(f"\nTesting rest_brId: {rest_brId} ({info.get('city')} - {info.get('area')})")
    
    params = {
        'restId': '55126',
        'rest_brId': str(rest_brId)
    }
    
    try:
        response = requests.get(
            'https://shop.imtiaz.com.pk/api/menu-section',
            headers=headers,
            params=params,
            timeout=10
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            menu_data = response.json()
            if menu_data.get('status') == 200:
                categories = menu_data.get('data', [])
                print(f"✓ SUCCESS! Found {len(categories)} categories")
            else:
                print(f"✗ API returned error status: {menu_data.get('status')}")
        else:
            print(f"✗ HTTP Error: {response.status_code}")
            
    except Exception as e:
        print(f"✗ Exception: {str(e)}")
    
    # Save the geofence map for reference
    with open('imtiaz_branch_map.json', 'w', encoding='utf-8') as f:
        json.dump(geofence_map, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*60)
    print("✓ Branch map saved to: imtiaz_branch_map.json")
    print("="*60)
    
else:
    print(f"✗ Failed to fetch geofences: {response.status_code}")