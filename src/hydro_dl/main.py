# hydro_dl/main.py
import logging
import os
import sys
import pathlib
from pathlib import Path
import time
from datetime import datetime, timezone
from dateutil import parser as dtparse
from dateutil.tz import gettz, UTC
from onc import ONC
from typing import List, Optional, Tuple

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
from .calibration import get_hydrophone_calibration
from .exceptions import ConfigError, UserAbortError, NoDataError, ONCInteractionError, HydroDLError, DownloadError 

# === Add function to write the new calibration data format ===
def write_calibration_data(
    device_code: str,
    sensitivity: List[float],
    bin_freqs: List[float], # Can be empty
    valid_from: Optional[datetime],
    valid_to: Optional[datetime],
    out_dir: Path
) -> None:
    """Write sensitivity and optional frequency bins to a calibration file."""
    out_path = out_dir / f"{device_code}-hydrophoneCalibration.txt"
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    try:
        with out_path.open("w") as f:
            f.write(f"# Hydrophone Calibration Data for Device: {device_code}\n")
            if valid_from:
                f.write(f"# Calibration valid from: {valid_from.isoformat()}\n")
            if valid_to:
                f.write(f"# Calibration valid to:   {valid_to.isoformat()}\n")
            else: # Indicate if no end date was found
                f.write("# Calibration valid until further notice.\n")

            # Determine header based on whether frequency bins are available
            if bin_freqs:
                f.write("# Columns: Frequency (Hz), Sensitivity (dB re 1uPa/sqrt(Hz) ? - check ONC docs)\n") # Clarify units if possible
            else:
                f.write("# Columns: Bin Index (1-based), Sensitivity (dB re 1uPa/sqrt(Hz) ? - check ONC docs)\n")

            f.write(f"# File generated: {ts_now}\n")
            f.write("# ------------------------------------\n")

            if bin_freqs:
                # Write Frequency, Sensitivity
                for freq, sens in zip(bin_freqs, sensitivity):
                    f.write(f"{freq:.2f}, {sens:.6f}\n") # Format frequency as needed
            else:
                # Write Index, Sensitivity
                for idx, sens in enumerate(sensitivity, start=1):
                    f.write(f"{idx}, {sens:.6f}\n")

        logging.info(f"Saved calibration file: {out_path.name} ({len(sensitivity)} points)")

    except IOError as e:
         logging.error(f"Failed to write calibration file {out_path}: {e}")
         # Optionally re-raise or handle differently
# === End write_calibration_data ===

def run_downloader() -> int:
    """Main function to run the hydrophone data downloader."""
    start_time = time.time()

    # 1. Parse Arguments
    args = hydro_args.setup_arg_parser()
    if args.debug_net:
        args.debug = True # debug_net implies debug

    # 2. Initial Setup
    try:
        # Token
        token = args.token or os.getenv("ONC_TOKEN")
        if not token:
            raise ConfigError("ONC API token required (--token or $ONC_TOKEN).")

        # Timezone
        local_zone = gettz(args.tz)
        if local_zone is None:
            raise ConfigError(f"Invalid timezone '{args.tz}'.")
        logging.info(f"Using local timezone: {args.tz}")

        # Date/Time Range
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
        start_utc = start_local.astimezone(UTC) # Use UTC from dateutil.tz
        end_utc = end_local.astimezone(UTC)   # Use UTC from dateutil.tz
        logging.info(f"Selected time window (Local: {args.tz}): {start_local.strftime('%Y-%m-%d %H:%M')} -> {end_local.strftime('%Y-%m-%d %H:%M')}")
        logging.info(f"Equivalent UTC window            : {utils.iso(start_utc)} -> {utils.iso(end_utc)}")

        # Output Directory
        output_path = pathlib.Path(args.output).resolve() # Resolve to absolute path early
        output_path.mkdir(parents=True, exist_ok=True)
        # Test writability
        test_file = output_path / f".hydro_dl_write_test_{os.getpid()}"
        test_file.touch(exist_ok=True)
        test_file.unlink()
        logging.info(f"Output directory: {output_path}")

        # Initialize ONC Client
        logging.info("Connecting to ONC...")
        onc_service = ONC(
            token,
            outPath=str(output_path),
            showInfo=args.debug_net,
            timeout=hydro_config.DEFAULT_ONC_TIMEOUT
        )
        # Simple test call to verify token/connection
        onc_service.getLocations({})
        utils.dbg(f"ONC client initialized successfully.", args=args)

    except (ConfigError, ValueError, AssertionError, PermissionError, dtparse.ParserError, requests.exceptions.RequestException, UserAbortError) as e:
         # Handle known setup errors
         logging.error(f"❌ Setup Error: {e}")
         if isinstance(e, UserAbortError): logging.info("Operation cancelled by user.")
         return 1
    except Exception as e: # Catch unexpected setup errors
         logging.error(f"❌ Unexpected Setup Error: {e}", exc_info=args.debug)
         return 1


    # --- Core Logic ---
    try:
        # 3. Discover Deployments & Products
        deployments, loc_map = onc.find_overlapping_deployments(onc_service, start_utc, end_utc, args)
        chosen_deps, parent_loc_code = onc.select_location_and_devices(deployments, loc_map) # Now returns parent code
        chosen_products = onc.select_data_products(onc_service, chosen_deps, args)

         # === Fetch Sensitivity Data (using new calibration module) ===
        if args.fetch_sensitivity:
            print("\n--- Fetching Sensitivity Calibration ---")
            # Get unique device codes from the final list of chosen deployments
            unique_device_codes_for_cal = sorted(list(set(dep.get('deviceCode') for dep in chosen_deps if dep.get('deviceCode'))))

            if not unique_device_codes_for_cal:
                 logging.warning("No device codes selected, cannot fetch sensitivity.")
            else:
                 logging.info(f"Attempting to fetch sensitivity for device(s): {', '.join(unique_device_codes_for_cal)}")
                 for device_code_cal in unique_device_codes_for_cal:
                     logging.info(f"Processing calibration for: {device_code_cal}")
                     try:
                         # Call the new function, passing start_utc as the reference date
                         is_cal, sens_data, bins_data, date_from, date_to = get_hydrophone_calibration(
                             onc_service,
                             device_code_cal,
                             date_in=start_utc # Use start time of data request
                         )

                         if is_cal:
                             try:
                                 # Call the new writing function
                                 write_calibration_data(
                                     device_code_cal,
                                     sens_data,
                                     bins_data,
                                     date_from,
                                     date_to,
                                     output_path
                                 )
                             except Exception as write_err:
                                 logging.error(f"Failed to write calibration file for {device_code_cal}: {write_err}")
                         else:
                             # Warning is logged within get_hydrophone_calibration if not found
                             pass
                     except Exception as fetch_err:
                          # Catch unexpected errors during the main calibration call
                          logging.error(f"Error processing calibration for {device_code_cal}: {fetch_err}", exc_info=args.debug)
            print("--- End Sensitivity Fetching ---")
        # === End Sensitivity Fetching ===

        # 4. Request Jobs from ONC (or prepare archive filters)
        jobs, total_bytes_est = onc.request_onc_jobs(onc_service, chosen_deps, chosen_products, start_utc, end_utc, args)
        logging.info(f"Prepared {len(jobs)} job(s)/archive request(s).")

        # 5. User Confirmation
        print(f"\nTotal estimated download size: {utils.human_size(total_bytes_est)}")
        if not args.yes and not args.test:  # Skip confirmation in test mode
            if not ui.confirm_proceed("Proceed with download?"):
                raise UserAbortError("Download cancelled by user confirmation.")

        # 6. Process Downloads (Now returns detailed status dict)
        # This function now handles both data product runs and archive downloads
        if args.test:
            print("\n=== TEST MODE: Listing Available Files ===")
        all_successful, job_statuses = onc.process_download_jobs(jobs, onc_service, output_path, args)

        end_time = time.time()
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")
        if args.debug_net:
            print("\n--- Final Contents of Output Directory ---")
            utils.list_tree(output_path, args=args)
            print("-" * 41)

        # Return overall success code
        return 0 if all_successful else 1

    # --- Error Handling for Core Logic ---
    except UserAbortError as e:
         logging.info(f"Operation cancelled: {e}")
         return 2 # Different exit code for user abort
    except NoDataError as e:
         # Log as warning because it's often not an error, just no data found
         logging.warning(f"Could not proceed: {e}")
         # Return success code as the script finished cleanly without finding data/jobs
         return 0
    except (ONCInteractionError, ConfigError, HydroDLError, DownloadError) as e:
         # Handle known application / API / Download errors
         logging.error(f"❌ Application Error: {e}", exc_info=args.debug)
         return 1
    except Exception as e:
         # Catch any truly unexpected exceptions during the core logic
         logging.error(f"❌ An unexpected error occurred: {e}", exc_info=True) # Always show traceback here
         return 1