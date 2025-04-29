# hydro_dl/main.py
import logging
import os
import sys
import pathlib
import time
from datetime import datetime
from dateutil import parser as dtparse
from dateutil.tz import gettz, UTC
from onc import ONC

# Perform essential imports and check for libraries early
try:
    import requests
except ImportError as e:
    # Error messages are now logged by the entry script (hydro_dl.py)
    # Re-raising here to ensure failure if somehow missed.
    raise ImportError(f"A required library is missing: {e}. Please install dependencies.") from e

# Import our own modules
from . import args as hydro_args
from . import config as hydro_config
from . import utils
from . import ui
from . import onc_client as onc
from .exceptions import ConfigError, UserAbortError, NoDataError, ONCInteractionError, HydroDLError


def run_downloader() -> int:
    """Main function to run the hydrophone data downloader."""
    start_time = time.time()

    # 1. Parse Arguments
    args = hydro_args.setup_arg_parser()
    if args.debug_net:
        args.debug = True # debug_net implies debug

    # 2. Initial Setup
    # Token
    token = args.token or os.getenv("ONC_TOKEN")
    if not token:
        logging.error("❌ An ONC API token is required. Provide via --token or $ONC_TOKEN environment variable.")
        return 1 # Return error code

    # Timezone
    try:
        local_zone = gettz(args.tz)
        if local_zone is None:
            raise ValueError(f"Timezone '{args.tz}' not found by dateutil.")
        logging.info(f"Using local timezone: {args.tz}")
    except Exception as e:
        logging.error(f"❌ Invalid timezone '{args.tz}': {e}")
        return 1

    # Date/Time Range
    try:
        if args.start and args.end:
            start_local = utils.parse_local(args.start, local_zone)
            end_local = utils.parse_local(args.end, local_zone)
            if end_local <= start_local:
                raise ValueError("End date/time must be after start date/time.")
        else:
            print(f"\nEnter the date/time range for data download.")
            print(f"Use the local timezone '{args.tz}'. Recommended format: YYYY-MM-DD HH:MM")
            start_local = None
            while start_local is None:
                try: start_local = utils.parse_local(input("Start Date/Time: "), local_zone)
                except Exception as e: print(f"✖ Invalid format or date. {e}. Please try again.")

            end_local = None
            while end_local is None:
                try:
                     end_local = utils.parse_local(input("End Date/Time:   "), local_zone)
                     if end_local <= start_local:
                         print("✖ End date/time must be after start. Please try again.")
                         end_local = None # Force re-entry
                except Exception as e: print(f"✖ Invalid format or date. {e}. Please try again.")

        # Convert to UTC for ONC API calls
        start_utc = start_local.astimezone(UTC)
        end_utc = end_local.astimezone(UTC)
        logging.info(f"Selected time window (Local: {args.tz}): {start_local.strftime('%Y-%m-%d %H:%M')} -> {end_local.strftime('%Y-%m-%d %H:%M')}")
        logging.info(f"Equivalent UTC window            : {utils.iso(start_utc)} -> {utils.iso(end_utc)}")

    except (ValueError, dtparse.ParserError, UserAbortError) as e:
         logging.error(f"❌ Error setting date/time range: {e}")
         if isinstance(e, UserAbortError): logging.info("Operation cancelled by user.")
         return 1
    except Exception as e: # Catch unexpected errors during date parsing
         logging.error(f"❌ Unexpected error processing dates: {e}", exc_info=args.debug)
         return 1

    # Output Directory
    output_path = pathlib.Path(args.output).resolve() # Resolve to absolute path early
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        # Test writability
        test_file = output_path / f".hydro_dl_write_test_{os.getpid()}"
        test_file.touch(exist_ok=True)
        test_file.unlink()
        logging.info(f"Output directory: {output_path}")
    except PermissionError:
         logging.error(f"❌ Permission denied: Cannot write to output directory '{output_path}'.")
         return 1
    except Exception as e:
        logging.error(f"❌ Error creating or accessing output directory '{output_path}': {e}")
        return 1

    # Initialize ONC Client
    try:
        logging.info("Connecting to ONC...")
        onc_service = ONC(
            token,
            outPath=str(output_path),
            showInfo=args.debug_net,
            timeout=hydro_config.DEFAULT_ONC_TIMEOUT
        )
        # Simple test call to verify token/connection
        onc_service.getLocations({})  # Just get all locations instead of testing with 'TEST'
        utils.dbg(f"ONC client initialized successfully.", args=args)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            logging.error(f"❌ ONC Authentication Failed (401): Check your API token.")
        else:
            logging.error(f"❌ ONC Connection HTTP Error: {e}", exc_info=args.debug)
        return 1
    except Exception as e:
        logging.error(f"❌ Failed to initialize ONC client: {e}", exc_info=args.debug)
        return 1

    # --- Core Logic ---
    try:
        # 3. Discover Deployments & Products
        deployments, loc_map = onc.find_overlapping_deployments(onc_service, start_utc, end_utc, args)
        chosen_deps, loc_code = onc.select_location_and_devices(deployments, loc_map)
        chosen_products = onc.select_data_products(onc_service, chosen_deps, args)

        # 4. Request Jobs from ONC
        jobs, total_bytes_est = onc.request_onc_jobs(onc_service, chosen_deps, chosen_products, start_utc, end_utc, args)
        logging.info(f"Created {len(jobs)} data request job(s).")

        # 5. User Confirmation
        print(f"\nTotal estimated download size: {utils.human_size(total_bytes_est)}")
        if not args.yes:
            if not ui.confirm_proceed("Proceed with download?"):
                raise UserAbortError("Download cancelled by user confirmation.")

        # 6. Process Downloads (Run jobs, download data, handle fallback)
        all_successful, job_statuses = onc.process_download_jobs(jobs, onc_service, output_path, args)

        # 7. Final Summary
        print("\n==============================")
        print("Processing Summary:")
        # Could add more details from job_statuses if needed
        if all_successful:
            print("✅ All requested jobs processed successfully!")
        else:
            print("⚠ Some jobs encountered errors during processing.")
            # Optionally list failed jobs here from job_statuses
            failed_jobs = [k for k, v in job_statuses.items() if "Success" not in v]
            if failed_jobs:
                 print("  Failed jobs details:")
                 for job_key in failed_jobs:
                      print(f"    - {job_key}: {job_statuses[job_key]}")

        print(f"Downloaded files are located in: {output_path}")
        end_time = time.time()
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")

        if args.debug_net:
            print("\n--- Final Contents of Output Directory ---")
            utils.list_tree(output_path, args=args)
            print("-" * 41)
        print("==============================")

        return 0 if all_successful else 1 # Return 0 on success, 1 on partial or full failure

    # --- Error Handling for Core Logic ---
    except UserAbortError as e:
         logging.info(f"Operation cancelled: {e}")
         return 2 # Different exit code for user abort
    except NoDataError as e:
         logging.warning(f"Could not proceed: {e}")
         # No need to return error code if no data is simply 'not found' vs an error
         return 0 # Or maybe a specific code like 3? Let's use 0 for no data found cleanly.
    except (ONCInteractionError, ConfigError, HydroDLError) as e:
         # Handle known application / API errors
         logging.error(f"❌ Application Error: {e}", exc_info=args.debug)
         return 1
    except Exception as e:
         # Catch any truly unexpected exceptions during the core logic
         logging.error(f"❌ An unexpected error occurred: {e}", exc_info=True) # Always show traceback here
         return 1