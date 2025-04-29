#!/usr/bin/env python3
"""
hydro_dl.py — CLI downloader for ONC hydrophone WAV / PNG / TXT data
"""
import sys
import logging

# Initialize logging early in case of import errors in the package
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

# Make the package directory findable if running directly from the root
# (You might handle this differently with proper installation/PYTHONPATH)
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from hydro_dl import main as hydro_main
except ImportError as e:
    logging.error(f"Failed to import the main application module: {e}")
    logging.error("Ensure the 'hydro_dl' directory exists and is in your Python path.")
    # Provide more specific error messages based on common import failures
    if 'requests' in str(e).lower():
        logging.error("ERROR: 'requests' library not found. Please install it: pip install requests")
    elif 'dateutil' in str(e).lower():
        logging.error("ERROR: 'python-dateutil' library not found. Please install it: pip install python-dateutil")
    elif 'onc' in str(e).lower():
        logging.error("ERROR: 'onc-python' library not found or import failed. Please install it: pip install onc-python")
    sys.exit(1)
except Exception as e:
    logging.error(f"An unexpected error occurred during initial import: {e}")
    sys.exit(1)


if __name__ == "__main__":
    try:
        return_code = hydro_main.run_downloader()
        sys.exit(return_code)
    except Exception as e:
        # Catch any unhandled exceptions from the main logic
        logging.error(f"An unexpected error occurred during execution: {e}")
        # Optionally print traceback if debugging is hard
        # import traceback
        # traceback.print_exc()
        sys.exit(1)