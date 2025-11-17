import os
import sys
import json
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('test_imtiaz.log')
        ]
    )

def test_imtiaz_location():
    print("\n" + "="*50)
    print("IMTIAZ LOCATION PARSER")
    print("="*50)
    try:
        from imtiaz.ImtiazLocation import ImtiazLocation
        event = {
            "use_proxy": False,
            "page_size": 999999,
            "offset": 0,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }

        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999

        parser = ImtiazLocation(event, MockContext())
        result = parser.gen_location()
        print(json.dumps(result, indent=2))

        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_imtiaz_menu():
    print("\n" + "="*50)
    print("IMTIAZ MENU PARSER")
    print("="*50)
    try:
        from imtiaz.ImtiazMenu import ImtiazMenu
        event = {
            "use_proxy": False,
            "page_size": 999999,
            "offset": 0,
            "offset_end": -1,
            "force_fetch": True,
            "goto_next_step": True,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }

        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999

        parser = ImtiazMenu(event, MockContext())
        result = parser.gen_menu()
        print(json.dumps(result, indent=2))

        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_imtiaz_post_menu():
    print("\n" + "="*50)
    print("IMTIAZ POST-MENU PARSER")
    print("="*50)
    try:
        from imtiaz.ImtiazPostMenu import ImtiazPostMenu
        event = {
            "use_proxy": False,
            "page_size": 999999,
            "offset": 0,
            "offset_end": -1,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }

        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999

        parser = ImtiazPostMenu(event, MockContext())
        result = parser.associate_missing_price()
        print(json.dumps(result, indent=2))

        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_imtiaz_csv():
    print("\n" + "="*50)
    print("IMTIAZ CSV GENERATOR")
    print("="*50)
    try:
        from imtiaz.ImtiazJsonToCsv import ImtiazJsonToCsv
        event = {
            "use_proxy": False,
            "page_size": 999999,
            "offset": 0,
            "offset_end": -1,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }

        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999

        parser = ImtiazJsonToCsv(event, MockContext())
        result = parser.parse_menu_csv()
        print(json.dumps(result, indent=2))

        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def run_full_flow():
    setup_logging()
    results = {
        "location": test_imtiaz_location(),
        "menu": test_imtiaz_menu(),
        "post_menu": test_imtiaz_post_menu(),
        "csv": test_imtiaz_csv()
    }

    print("\n" + "="*50)
    print("PROCESS SUMMARY")
    print("="*50)
    for k, v in results.items():
        print(f"{k.upper():<12}: {'SUCCESS' if v else 'FAILED'}")

if __name__ == "__main__":
    print("="*60)
    print("IMTIAZ PARSER UTILITIES")
    print("="*60)

    print("\nSelect an option:")
    print("1. Full Flow")
    print("2. Location Only")
    print("3. Menu Only")
    print("4. Post Menu Only")
    print("5. CSV Only")
    print("6. Exit")

    choice = input("\nEnter your choice (1-6): ").strip()
    setup_logging()

    if choice == "1":
        run_full_flow()
    elif choice == "2":
        test_imtiaz_location()
    elif choice == "3":
        test_imtiaz_menu()
    elif choice == "4":
        test_imtiaz_post_menu()
    elif choice == "5":
        test_imtiaz_csv()
    elif choice == "6":
        print("Exiting...")
        sys.exit()
    else:
        print("Invalid input")
