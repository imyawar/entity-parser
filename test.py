import os
import sys
import json
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from common.ActionName import ActionName
except ImportError:
    class ActionName:
        PROCESS_LOCATION = "PROCESS_LOCATION"
        PROCESS_MENU = "PROCESS_MENU"
        PROCESS_POST_MENU = "PROCESS_POST_MENU"
        MAKE_CSV = "MAKE_CSV"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('test_metro.log')
        ]
    )


def check_metro_prerequisites():
    print("\n" + "=" * 50)
    print("CHECKING METRO PREREQUISITES")
    print("=" * 50)

    location_csv = "metro/data/in/metro_locations.csv"
    if os.path.exists(location_csv):
        print(f"Location CSV found: {location_csv}")
        with open(location_csv, 'r') as f:
            content = f.read()
            print(f"Contents: {content.strip()}")
    else:
        print(f"Missing location CSV: {location_csv}")
        print("Creating it now...")
        os.makedirs(os.path.dirname(location_csv), exist_ok=True)
        with open(location_csv, 'w') as f:
            f.write("id,message\n1,all\n")
        print("Created location CSV")

    folders = [
        "metro/data/in",
        "metro/data/location",
        "metro/data/menu",
        "metro/data/post-menu",
        "metro/data/post-menu-cost",
        "metro/data/result",
        "metro/data/status",
        "metro/data/failed_loc",
        "metro/data/failed_menu"
    ]

    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
            print(f"Created folder: {folder}")
        else:
            try:
                file_count = len([f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))])
                print(f"Folder exists: {folder} ({file_count} files)")
            except:
                print(f"Folder exists: {folder}")

    print("\nAll prerequisites checked/created")
    return True


def test_metro(action, page_size=999999, offset=0, use_proxy=False):
    print("\n" + "=" * 60)
    print(f"TESTING METRO - ACTION: {action}")
    print("=" * 60)

    try:
        event = {
            "use_proxy": use_proxy,
            "page_size": page_size,
            "offset": offset,
            "offset_end": -1,
            "force_fetch": True,
            "goto_next_step": True,
            "log_id": f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }

        class MockContext:
            def get_remaining_time_in_millis(self):
                return 999999999

        context = MockContext()
        result = None

        if action == "PROCESS_LOCATION" or action == ActionName.PROCESS_LOCATION:
            from metro.MetroLocation import MetroLocation
            parser = MetroLocation(event, context)
            result = parser.gen_location()

        elif action == "PROCESS_MENU" or action == ActionName.PROCESS_MENU:
            from metro.MetroMenu import MetroMenu
            parser = MetroMenu(event, context)
            result = parser.gen_menu()

        elif action == "PROCESS_POST_MENU" or action == ActionName.PROCESS_POST_MENU:
            from metro.MetroPostMenu import MetroPostMenu
            parser = MetroPostMenu(event, context)
            result = parser.associate_missing_price()

        elif action == "MAKE_CSV" or action == ActionName.MAKE_CSV:
            from metro.MetroJsonToCsv import MetroJsonToCsv
            parser = MetroJsonToCsv(event, context)
            result = parser.parse_menu_csv()

        else:
            print(f"Unknown action: {action}")
            return False

        print("\n" + "=" * 60)
        print("RESULT")
        print("=" * 60)
        print(json.dumps(result, indent=2))
        print("\nTest completed successfully")
        return True

    except KeyboardInterrupt:
        print("\nTest stopped by user")
        return True
    except Exception as e:
        print(f"\nError during test: {str(e)}")
        logging.exception(f"Test failed for action {action}")
        return False


def run_metro_full_flow():
    print("\n" + "=" * 60)
    print("METRO PARSER - FULL FLOW")
    print("=" * 60)

    setup_logging()

    if not check_metro_prerequisites():
        return False

    results = {}

    try:
        print("\n" + "=" * 60)
        print("STEP 1: LOCATION SCRAPING")
        print("=" * 60)
        results['location'] = test_metro("PROCESS_LOCATION")
        if not results['location']:
            print("\nLocation step failed. Stopping.")
            return False

        print("\n" + "=" * 60)
        print("STEP 2: MENU SCRAPING")
        print("=" * 60)
        results['menu'] = test_metro("PROCESS_MENU")
        if not results['menu']:
            print("\nMenu step failed. Stopping.")
            return False

        print("\n" + "=" * 60)
        print("STEP 3: POST MENU PROCESSING")
        print("=" * 60)
        results['post_menu'] = test_metro("PROCESS_POST_MENU")
        if not results['post_menu']:
            print("\nPost menu step failed. Stopping.")
            return False

        print("\n" + "=" * 60)
        print("STEP 4: CSV GENERATION")
        print("=" * 60)
        results['csv'] = test_metro("MAKE_CSV")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        for step, passed in results.items():
            status = "PASSED" if passed else "FAILED"
            print(f"{step.upper():<15} {status}")

        all_passed = all(results.values())
        if all_passed:
            result_folder = "metro/data/result"
            if os.path.exists(result_folder):
                csv_files = [f for f in os.listdir(result_folder) if f.endswith('.csv')]
                if csv_files:
                    print(f"\nOutput CSV files: {len(csv_files)}")
                    for csv_file in csv_files:
                        file_path = os.path.join(result_folder, csv_file)
                        size = os.path.getsize(file_path)
                        print(f" - {csv_file} ({size:,} bytes)")
        else:
            print("\nSome steps failed. Check logs for details.")

        return all_passed

    except KeyboardInterrupt:
        print("\nFlow stopped by user")
        return False
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        logging.exception("Full flow failed")
        return False


def show_metro_menu():
    print("\n" + "=" * 60)
    print("METRO PARSER TEST SUITE")
    print("=" * 60)

    while True:
        print("\nChoose test option:")
        print("1. Full flow (Location -> Menu -> Post Menu -> CSV)")
        print("2. Test Location only")
        print("3. Test Menu only")
        print("4. Test Post Menu only")
        print("5. Test CSV only")
        print("6. Exit")

        choice = input("\nEnter choice (1-6): ").strip()

        try:
            if choice == "1":
                run_metro_full_flow()
            elif choice == "2":
                test_metro("PROCESS_LOCATION")
            elif choice == "3":
                test_metro("PROCESS_MENU")
            elif choice == "4":
                test_metro("PROCESS_POST_MENU")
            elif choice == "5":
                test_metro("MAKE_CSV")
            elif choice == "6":
                break
            else:
                print("Invalid choice. Please enter 1-6.")
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            continue


def main():
    if len(sys.argv) > 1:
        parser_name = sys.argv[1].lower()
        action = sys.argv[2].upper() if len(sys.argv) > 2 else "PROCESS_LOCATION"

        if parser_name == "metro":
            setup_logging()
            check_metro_prerequisites()

            if action == "FULL":
                run_metro_full_flow()
            else:
                test_metro(action)
        else:
            print(f"Unknown parser: {parser_name}")
            print("Usage: python test.py metro [ACTION]")
            print("Actions: PROCESS_LOCATION, PROCESS_MENU, PROCESS_POST_MENU, MAKE_CSV, FULL")
    else:
        show_metro_menu()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest session ended by user")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        logging.exception("Main execution failed")
    finally:
        print("\n" + "=" * 60)
        print("Check 'test_metro.log' for detailed logs")
        print("=" * 60)