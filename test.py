import logging
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Import the Imtiaz location parser
from imtiaz.ImtiazLocation import ImtiazLocation

class MockContext:
    """Mock Lambda context for local testing"""
    def get_remaining_time_in_millis(self):
        return 300000  # 5 minutes

def test_imtiaz_location():
    """Test Imtiaz location fetching"""
    
    # Create event (simulates Lambda event)
    event = {
        "use_proxy": False,
        "page_size": 1,
        "offset": 0
    }
    
    # Create mock context
    context = MockContext()
    
    print("="*60)
    print("Testing Imtiaz Location Parser")
    print("="*60)
    
    try:
        # Initialize parser
        parser = ImtiazLocation(event, context)
        
        # Run location fetching
        result = parser.gen_location()
        
        print("\n" + "="*60)
        print("RESULT:")
        print("="*60)
        print(result)
        print("\n")
        
        # Check output
        output_path = "imtiaz/data/location"
        if os.path.exists(output_path):
            files = os.listdir(output_path)
            print(f"Total location files created: {len(files)}")
            print(f"Location: {output_path}")
            if len(files) > 0:
                print(f"\nSample files:")
                for f in files[:5]:  # Show first 5 files
                    print(f"  - {f}")
        else:
            print("No output folder created yet")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_imtiaz_location()