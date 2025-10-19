import os
import sys
import json
import logging
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('test_imtiaz.log')
        ]
    )

def test_imtiaz_location():
    """Test Imtiaz location parser"""
    print("\n" + "="*50)
    print("TESTING IMTIAZ LOCATION PARSER")
    print("="*50)
    
    try:
        from imtiaz.ImtiazLocation import ImtiazLocation
        
        # Mock event and context for testing - NO RESTRICTIONS
        event = {
            "use_proxy": False,
            "page_size": 999999,  # Process all locations
            "offset": 0,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999  # Essentially unlimited time
        
        context = MockContext()
        
        # Initialize parser
        parser = ImtiazLocation(event, context)
        
        print(f"Service name: {parser.get_service_name()}")
        print(f"Running in Lambda: {parser.running_in_lambda}")
        print(f"Local data path: {parser.local_data_path}")
        
        # Test location generation
        print("\nStarting location generation...")
        print("Processing ALL locations. Press Ctrl+C to stop manually.")
        result = parser.gen_location()
        
        print("\nLocation generation completed!")
        print(f"Result: {json.dumps(result, indent=2)}")
        
        return True
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Location generation stopped by user (Ctrl+C)")
        return True
    except Exception as e:
        print(f"Error testing Imtiaz location: {str(e)}")
        logging.exception("Location test failed")
        return False

def test_imtiaz_menu():
    """Test Imtiaz menu parser"""
    print("\n" + "="*50)
    print("TESTING IMTIAZ MENU PARSER")
    print("="*50)
    
    try:
        from imtiaz.ImtiazMenu import ImtiazMenu
        
        # Mock event and context for testing - NO RESTRICTIONS
        event = {
            "use_proxy": False,
            "page_size": 999999,  # Process all stores
            "offset": 0,
            "offset_end": -1,  # No end limit
            "force_fetch": True,
            "goto_next_step": True,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999  # Essentially unlimited time
        
        context = MockContext()
        
        # Initialize parser
        parser = ImtiazMenu(event, context)
        
        print(f"Service name: {parser.get_service_name()}")
        print(f"Running in Lambda: {parser.running_in_lambda}")
        print(f"Input file path: {parser.input_file_path}")
        
        # Test menu generation
        print("\nStarting menu generation...")
        print("Processing ALL stores. Press Ctrl+C to stop manually.")
        result = parser.gen_menu()
        
        print("\nMenu generation completed!")
        print(f"Result: {json.dumps(result, indent=2)}")
        
        return True
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Menu generation stopped by user (Ctrl+C)")
        return True
    except Exception as e:
        print(f"Error testing Imtiaz menu: {str(e)}")
        logging.exception("Menu test failed")
        return False

def test_imtiaz_post_menu():
    """Test Imtiaz post-menu parser"""
    print("\n" + "="*50)
    print("TESTING IMTIAZ POST-MENU PARSER")
    print("="*50)
    
    try:
        from imtiaz.ImtiazPostMenu import ImtiazPostMenu
        
        # Mock event and context for testing - NO RESTRICTIONS
        event = {
            "use_proxy": False,
            "page_size": 999999,  # Process all menu files
            "offset": 0,
            "offset_end": -1,  # No end limit
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999  # Essentially unlimited time
        
        context = MockContext()
        
        # Initialize parser
        parser = ImtiazPostMenu(event, context)
        
        print(f"Service name: {parser.get_service_name()}")
        print(f"Running in Lambda: {parser.running_in_lambda}")
        print(f"Input file path: {parser.input_file_path}")
        
        # Test post-menu generation
        print("\nStarting post-menu generation...")
        print("Processing ALL menu files. Press Ctrl+C to stop manually.")
        result = parser.associate_missing_price()
        
        print("\nPost-menu generation completed!")
        print(f"Result: {json.dumps(result, indent=2)}")
        
        return True
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Post-menu generation stopped by user (Ctrl+C)")
        return True
    except Exception as e:
        print(f"Error testing Imtiaz post-menu: {str(e)}")
        logging.exception("Post-menu test failed")
        return False

def test_imtiaz_csv():
    """Test Imtiaz CSV generation"""
    print("\n" + "="*50)
    print("TESTING IMTIAZ CSV GENERATION")
    print("="*50)
    
    try:
        from imtiaz.ImtiazJsonToCsv import ImtiazJsonToCsv
        
        # Mock event and context for testing - NO RESTRICTIONS
        event = {
            "use_proxy": False,
            "page_size": 999999,  # Process all files
            "offset": 0,
            "offset_end": -1,  # No end limit
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999  # Essentially unlimited time
        
        context = MockContext()
        
        # Initialize parser
        parser = ImtiazJsonToCsv(event, context)
        
        print(f"Service name: {parser.get_service_name()}")
        print(f"Running in Lambda: {parser.running_in_lambda}")
        print(f"Input file path: {parser.input_file_path}")
        
        # Test CSV generation
        print("\nStarting CSV generation...")
        print("Processing ALL menu files. Press Ctrl+C to stop manually.")
        result = parser.parse_menu_csv()
        
        print("\nCSV generation completed!")
        print(f"Result: {json.dumps(result, indent=2)}")
        
        return True
        
    except KeyboardInterrupt:
        print("\n\n⚠️ CSV generation stopped by user (Ctrl+C)")
        return True
    except Exception as e:
        print(f"Error testing Imtiaz CSV: {str(e)}")
        logging.exception("CSV test failed")
        return False

def check_prerequisites():
    """Check if required files and folders exist"""
    print("\n" + "="*50)
    print("CHECKING PREREQUISITES")
    print("="*50)
    
    prerequisites_met = True
    
    # Check if location CSV exists
    location_csv = "imtiaz/data/in/imtiaz_locations.csv"
    if os.path.exists(location_csv):
        print(f"✓ Location CSV found: {location_csv}")
        
        # Show contents
        with open(location_csv, 'r') as f:
            content = f.read()
            print(f"  Contents: {content.strip()}")
    else:
        print(f"✗ Missing location CSV: {location_csv}")
        print("  Creating it now...")
        os.makedirs(os.path.dirname(location_csv), exist_ok=True)
        with open(location_csv, 'w') as f:
            f.write("id,message\n1,all\n")
        print("  ✓ Created location CSV")
    
    # Check data folders
    folders = [
        "imtiaz/data/in",
        "imtiaz/data/location", 
        "imtiaz/data/menu",
        "imtiaz/data/post-menu",
        "imtiaz/data/result",
        "imtiaz/data/status",
        "imtiaz/data/failed_loc",
        "imtiaz/data/failed_menu"
    ]
    
    for folder in folders:
        if os.path.exists(folder):
            # Count files in folder
            try:
                file_count = len([f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))])
                print(f"✓ Folder exists: {folder} ({file_count} files)")
            except:
                print(f"✓ Folder exists: {folder}")
        else:
            print(f"✗ Missing folder: {folder}")
            os.makedirs(folder, exist_ok=True)
            print(f"  ✓ Created folder: {folder}")
    
    # Check common data files
    common_files = [
        "data/cbsa_bounding_boxes.json",
        "data/cbsa_db.csv"
    ]
    
    for file in common_files:
        if os.path.exists(file):
            print(f"✓ Common file exists: {file}")
        else:
            print(f"⚠ Common file missing: {file}")
            # These are not critical for basic testing
    
    return prerequisites_met

def run_full_test_flow():
    """Run complete test flow for Imtiaz parser"""
    print("IMTIAZ PARSER - FULL TEST FLOW")
    print("="*60)
    print("⚠️  This will process ALL stores without limits!")
    print("⚠️  Press Ctrl+C at any time to stop manually.")
    print("="*60)
    
    # Setup logging
    setup_logging()
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please fix the issues above.")
        return False
    
    results = {
        'location': False,
        'menu': False,
        'post_menu': False,
        'csv': False
    }
    
    try:
        # Test Location Parser
        print("\n" + "="*60)
        print("STEP 1: TESTING LOCATION PARSER")
        print("="*60)
        results['location'] = test_imtiaz_location()
        
        # Test Menu Parser (only if location was successful)
        if results['location']:
            print("\n" + "="*60)
            print("STEP 2: TESTING MENU PARSER (Structure Only)") 
            print("="*60)
            results['menu'] = test_imtiaz_menu()
        
        # Test Post-Menu Parser (only if menu was successful)
        if results['menu']:
            print("\n" + "="*60)
            print("STEP 3: TESTING POST-MENU PARSER (Fetch Products)")
            print("="*60)
            results['post_menu'] = test_imtiaz_post_menu()
        
        # Test CSV Generation (only if post-menu was successful)
        if results['post_menu']:
            print("\n" + "="*60)
            print("STEP 4: TESTING CSV GENERATION")
            print("="*60)
            results['csv'] = test_imtiaz_csv()
        
        # Print summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        
        for test_name, passed in results.items():
            status = "✓ PASSED" if passed else "✗ FAILED"
            print(f"{test_name.upper():<15} {status}")
        
        all_passed = all(results.values())
        if all_passed:
            print("\n🎉 ALL TESTS PASSED! IMTIAZ PARSER IS WORKING CORRECTLY.")
        else:
            print("\n❌ SOME TESTS FAILED. CHECK THE LOGS FOR DETAILS.")
        
        return all_passed
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Full test flow stopped by user (Ctrl+C)")
        print("\nPartial results:")
        for test_name, passed in results.items():
            status = "✓ PASSED" if passed else "✗ NOT RUN"
            print(f"{test_name.upper():<15} {status}")
        return False
    except Exception as e:
        print(f"\n💥 UNEXPECTED ERROR: {str(e)}")
        logging.exception("Full test flow failed")
        return False

def test_specific_functionality():
    """Test specific functionality if needed"""
    print("\n" + "="*50)
    print("TESTING SPECIFIC FUNCTIONALITY")
    print("="*50)
    
    try:
        # Test API calls directly
        import requests
        
        # Test geofence API
        print("Testing Imtiaz geofence API...")
        url = "https://shop.imtiaz.com.pk/api/geofence"
        params = {'restId': '55126'}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'Accept': 'application/json, text/plain, */*',
            'app-name': 'imtiazsuperstore', 
            'rest-id': '55126',
            'Referer': 'https://shop.imtiaz.com.pk/'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            cities = data.get('data', {}).get('cities', [])
            print(f"✓ Geofence API successful - Found {len(cities)} cities")
            
            total_locations = 0
            for city in cities:
                total_locations += len(city.get('geofences', []))
            print(f"✓ Total locations found: {total_locations}")
        else:
            print(f"✗ Geofence API failed: {response.status_code}")
        
        # Test menu-section API
        print("\nTesting Imtiaz menu-section API...")
        menu_url = "https://shop.imtiaz.com.pk/api/menu-section"
        menu_params = {'restId': '55126', 'rest_brId': '54943', 'delivery_type': '0', 'source': ''}
        menu_response = requests.get(menu_url, headers=headers, params=menu_params, timeout=10)
        if menu_response.status_code == 200:
            menu_data = menu_response.json()
            menus = menu_data.get('data', [])
            print(f"✓ Menu-section API successful - Found {len(menus)} menu sections")
        else:
            print(f"✗ Menu-section API failed: {menu_response.status_code}")
            
    except Exception as e:
        print(f"Error in specific functionality test: {str(e)}")

if __name__ == "__main__":
    print("="*60)
    print("IMTIAZ PARSER TEST SUITE")
    print("="*60)
    print("⚠️  All tests will process UNLIMITED records!")
    print("⚠️  Press Ctrl+C anytime to stop manually.")
    print("="*60)
    
    # Ask user what they want to test
    print("\nChoose test option:")
    print("1. Full test flow (Location → Menu → Post-Menu → CSV)")
    print("2. Test location only") 
    print("3. Test menu only (structure)")
    print("4. Test post-menu only (fetch products)")
    print("5. Test CSV only")
    print("6. Test specific functionality")
    print("7. Check prerequisites only")
    
    choice = input("\nEnter your choice (1-7): ").strip()
    
    try:
        if choice == "1":
            run_full_test_flow()
        elif choice == "2":
            test_imtiaz_location()
        elif choice == "3":
            test_imtiaz_menu()
        elif choice == "4":
            test_imtiaz_post_menu()
        elif choice == "5":
            test_imtiaz_csv()
        elif choice == "6":
            test_specific_functionality()
        elif choice == "7":
            check_prerequisites()
        else:
            print("Invalid choice. Running full test flow...")
            run_full_test_flow()
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted by user (Ctrl+C)")
    
    print("\n" + "="*60)
    print("Test session ended. Check 'test_imtiaz.log' for detailed logs.")
    print("="*60)