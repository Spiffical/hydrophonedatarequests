"""
Main downloader module for hydrophone data retrieval
"""
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
from typing import List, Optional, Tuple, Dict, Any

# Perform essential imports and check for libraries early
try:
    import requests
    import ipywidgets as widgets # For type hints
except ImportError as e:
    # Error messages are now logged by the entry script
    raise ImportError(f"A required library is missing: {e}. Please install dependencies.") from e

# Import our own modules
from hydrophone.cli import args as cli_args
from hydrophone.config import settings
from hydrophone.utils import helpers as utils
from hydrophone.cli import ui
from hydrophone.core import onc_client as onc
from hydrophone.core.calibration import get_hydrophone_calibration
from hydrophone.utils.exceptions import (
    ConfigError, UserAbortError, NoDataError,
    ONCInteractionError, HydroDLError, DownloadError
)

# --- Add OutputWidgetHandler for Colab UI ---
class OutputWidgetHandler(logging.Handler):
    """Custom logging handler to direct logs to an ipywidgets.Output widget."""
    def __init__(self, output_widget: 'widgets.Output', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = output_widget

    def emit(self, record):
        """Overload of logging.Handler method."""
        formatted_record = self.format(record)
        with self.out:
            # Append the formatted record to the Output widget
            # Add newline if not already present
            print(formatted_record, end='\n' if not formatted_record.endswith('\n') else '')

    def handle_error(self, record):
        # Fallback to stderr if there's an error within the handler itself
        super().handle_error(record)

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
                f.write("# Columns: Frequency (Hz), Sensitivity (dB re 1uPa/sqrt(Hz) ? - check ONC docs)\n")
            else:
                f.write("# Columns: Bin Index (1-based), Sensitivity (dB re 1uPa/sqrt(Hz) ? - check ONC docs)\n")

            f.write(f"# File generated: {ts_now}\n")
            f.write("# ------------------------------------\n")

            if bin_freqs:
                # Write Frequency, Sensitivity
                for freq, sens in zip(bin_freqs, sensitivity):
                    f.write(f"{freq:.2f}, {sens:.6f}\n")
            else:
                # Write Index, Sensitivity
                for idx, sens in enumerate(sensitivity, start=1):
                    f.write(f"{idx}, {sens:.6f}\n")

        logging.info(f"Saved calibration file: {out_path.name} ({len(sensitivity)} points)")

    except IOError as e:
         logging.error(f"Failed to write calibration file {out_path}: {e}")

def run_download_logic(params: Dict[str, Any], output_widget: Optional['widgets.Output'] = None) -> int:
    """
    Core logic for the downloader. Skips setup/discovery if 'onc_service'
    and 'chosen_deployments' are provided in params.
    """
    start_time = time.time()
    logger = logging.getLogger()

    # --- Setup logging ---
    log_level = logging.DEBUG if params.get('debug', False) or params.get('debug_net', False) else logging.INFO
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(log_level)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    notebook_handler = None
    if output_widget:
        notebook_handler = OutputWidgetHandler(output_widget)
        notebook_handler.setFormatter(log_formatter)
        logger.addHandler(notebook_handler)

    # --- Check if we should SKIP Setup/Discovery ---
    skip_discovery = ('onc_service' in params and 'chosen_deployments' in params and 
                     params['onc_service'] is not None and params['chosen_deployments'] is not None)
    onc_service = None
    chosen_deps = []
    parent_loc_code = None
    chosen_products = {}
    start_utc = None
    end_utc = None
    output_path = None

    if skip_discovery:
        logging.info("Skipping setup and discovery - using provided parameters.")
        onc_service = params['onc_service']
        chosen_deps = params['chosen_deployments']
        chosen_products = params.get('chosen_products', {})
        start_utc = params['start_dt'].astimezone(UTC)
        end_utc = params['end_dt'].astimezone(UTC)
        output_path_str = params.get('output', 'downloads')
        output_path = pathlib.Path(output_path_str).resolve()
        output_path.mkdir(parents=True, exist_ok=True)
        fake_args = type('obj', (object,), params)()
    else:
        # --- Perform Full Setup and Discovery (CLI or first run) ---
        logging.info("Performing full setup and discovery...")
        try:
            # 1. Validate and Prepare Parameters
            token = params.get('token')
            if not token:
                raise ConfigError("ONC API token required ('token').")

            tz_str = params.get('tz', 'America/Vancouver')
            local_zone = gettz(tz_str)
            if local_zone is None:
                raise ConfigError(f"Invalid timezone '{tz_str}'.")
            logging.info(f"Using local timezone: {tz_str}")

            start_local = params.get('start_dt')
            end_local = params.get('end_dt')
            # If datetimes aren't objects, try parsing from strings (CLI case)
            if not isinstance(start_local, datetime) and params.get('start_str'):
                start_local = utils.parse_local(params['start_str'], local_zone)
            if not isinstance(end_local, datetime) and params.get('end_str'):
                end_local = utils.parse_local(params['end_str'], local_zone)

            if not isinstance(start_local, datetime) or not isinstance(end_local, datetime):
                raise ConfigError("Start and end datetimes must be provided.")
            if not start_local.tzinfo or not end_local.tzinfo:
                start_local = start_local.replace(tzinfo=local_zone)
                end_local = end_local.replace(tzinfo=local_zone)
            if end_local <= start_local:
                raise ValueError("End date/time must be after start date/time.")

            start_utc = start_local.astimezone(UTC)
            end_utc = end_local.astimezone(UTC)
            logging.info(f"Selected time window (Local: {tz_str}): {start_local.strftime('%Y-%m-%d %H:%M:%S %Z')} -> {end_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logging.info(f"Equivalent UTC window              : {utils.iso(start_utc)} -> {utils.iso(end_utc)}")

            output_path_str = params.get('output', 'downloads')
            output_path = pathlib.Path(output_path_str).resolve()
            output_path.mkdir(parents=True, exist_ok=True)
            test_file = output_path / f".hydro_dl_write_test_{os.getpid()}"
            test_file.touch(exist_ok=True)
            test_file.unlink()
            logging.info(f"Output directory: {output_path}")

            logging.info("Connecting to ONC...")
            onc_service = ONC(
                token,
                outPath=str(output_path),
                showInfo=params.get('debug_net', False),
                timeout=settings.DEFAULT_ONC_TIMEOUT
            )
            onc_service.getLocations({})
            utils.dbg_param("ONC client initialized successfully.", debug_on=params.get('debug', False))

            # 2. Discover Deployments & Products
            fake_args = type('obj', (object,), params)()
            deployments, loc_map = onc.find_overlapping_deployments(onc_service, start_utc, end_utc, fake_args)

            # CLI INTERACTION - Only if not skipping discovery
            logging.info("Proceeding with interactive selection (CLI mode)...")
            chosen_deps, parent_loc_code = onc.select_location_and_devices(deployments, loc_map)
            is_archive_mode = params.get('archive', False)
            if not is_archive_mode:
                chosen_products = onc.select_data_products(onc_service, chosen_deps, fake_args)
            else:
                chosen_products = {}
            params['chosen_deployments'] = chosen_deps
            params['chosen_products'] = chosen_products

        except (ConfigError, ValueError, AssertionError, PermissionError, dtparse.ParserError,
                requests.exceptions.RequestException, UserAbortError) as e:
            logging.error(f"❌ Setup/Discovery Error: {e}")
            if isinstance(e, UserAbortError):
                logging.info("Operation cancelled by user.")
            return 1
        except Exception as e:
            logging.error(f"❌ Unexpected Setup/Discovery Error: {e}", exc_info=params.get('debug', False))
            return 1

    # --- Core Logic (Run AFTER setup/discovery OR if skipped) ---
    try:
        if not onc_service or not chosen_deps or start_utc is None or end_utc is None or output_path is None:
            raise HydroDLError("Core logic called without necessary parameters (service, deployments, dates, output path).")

        # === Fetch Sensitivity Data (Check if needed) ===
        if params.get('fetch_sensitivity', False):
            logging.info("\n--- Fetching Sensitivity Calibration ---")
            unique_device_codes_for_cal = sorted(list(set(dep.get('deviceCode') for dep in chosen_deps if dep.get('deviceCode'))))
            if not unique_device_codes_for_cal:
                logging.warning("No device codes selected, cannot fetch sensitivity.")
            else:
                logging.info(f"Attempting sensitivity for: {', '.join(unique_device_codes_for_cal)}")
                for device_code_cal in unique_device_codes_for_cal:
                    logging.info(f"Processing calibration for: {device_code_cal}")
                    try:
                        is_cal, sens_data, bins_data, date_from, date_to = get_hydrophone_calibration(
                            onc_service, device_code_cal, date_in=start_utc
                        )
                        if is_cal:
                            write_calibration_data(device_code_cal, sens_data, bins_data, date_from, date_to, output_path)
                    except Exception as fetch_err:
                        logging.error(f"Error processing calibration for {device_code_cal}: {fetch_err}",
                                    exc_info=params.get('debug', False))
            logging.info("--- End Sensitivity Fetching ---")

        # 3. Request Jobs from ONC (Use determined chosen_products)
        jobs, total_bytes_est = onc.request_onc_jobs(onc_service, chosen_deps, chosen_products, start_utc, end_utc, fake_args)
        logging.info(f"Prepared {len(jobs)} job(s)/archive request(s).")
        logging.info(f"Total estimated download size: {utils.human_size(total_bytes_est)}")

        # 4. User Confirmation (Handled by UI layer before calling this)
        if not params.get('yes', False) and not params.get('test', False) and not skip_discovery:
            if not ui.confirm_proceed("Proceed with download?"):
                raise UserAbortError("Download cancelled by user confirmation.")

        # 5. Process Downloads
        if params.get('test', False) and params.get('archive', False):
            logging.info("\n=== TEST MODE: Archive file listing completed (see request step). Skipping download. ===")
            all_successful = True
        elif params.get('test', False):
            logging.info("\n=== TEST MODE: Data product requests prepared. Skipping run/download. ===")
            all_successful = True
        else:
            all_successful, job_statuses = onc.process_download_jobs(jobs, onc_service, output_path, fake_args)

        end_time = time.time()
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")

        return 0 if all_successful else 1

    except UserAbortError as e:
        logging.info(f"Operation cancelled: {e}")
        return 2
    except NoDataError as e:
        logging.warning(f"Could not proceed: {e}")
        return 0
    except NotImplementedError as e:
        logging.error(f"Missing required interaction: {e}")
        return 1
    except (ONCInteractionError, ConfigError, HydroDLError, DownloadError) as e:
        logging.error(f"❌ Application Error: {e}", exc_info=params.get('debug', False))
        return 1
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred: {e}", exc_info=True)
        return 1
    finally:
        if notebook_handler:
            logger.removeHandler(notebook_handler)
            notebook_handler.close()
        logger.removeHandler(console_handler)
        console_handler.close()

def run_downloader() -> int:
    """Parses CLI args and calls the main logic function."""
    args = cli_args.setup_arg_parser()
    params = vars(args)

    # Add ONC_TOKEN from env if not provided via args
    if not params.get('token'):
        params['token'] = os.getenv("ONC_TOKEN")

    # For CLI, ensure dates are parsed if provided
    if args.start and args.end:
        try:
            local_zone = gettz(args.tz)
            if local_zone is None:
                raise ValueError(f"Invalid timezone '{args.tz}'")
            params['start_str'] = args.start
            params['end_str'] = args.end
            params['start_dt'] = utils.parse_local(args.start, local_zone)
            params['end_dt'] = utils.parse_local(args.end, local_zone)
        except Exception as e:
            print(f"ERROR: Invalid date/time or timezone input: {e}", file=sys.stderr)
            return 1
    elif not args.start or not args.end:
        print("\nError: Start and End date/time must be provided via --start and --end for CLI usage.", file=sys.stderr)
        return 1

    return run_download_logic(params, output_widget=None)