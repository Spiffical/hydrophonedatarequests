# hydro_dl/onc_client.py
import logging
import re
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import pprint
try:
    from dateutil import parser as dtparse
    from dateutil.tz import gettz, UTC
except ImportError:
    # This error should ideally be caught at the entry point,
    # but adding a check here for robustness.
    import sys
    sys.exit("ERROR: 'python-dateutil' library not found. Please install it: pip install python-dateutil")

try:
    from onc import ONC
    from onc.modules._DataProductFile import _DataProductFile
    from requests.exceptions import HTTPError
except ImportError as e:
    import sys
    if 'onc' in str(e).lower(): sys.exit("ERROR: 'onc-python' library not found or import failed. Please install it: pip install onc-python")
    elif 'requests' in str(e).lower(): sys.exit("ERROR: 'requests' library not found. Please install it: pip install requests")
    else: sys.exit(f"ERROR: Required library not found: {e}.")
except AttributeError as ae:
     import sys
     if '_DataProductFile' in str(ae): sys.exit("ERROR: Failed to import internal class from 'onc-python'. Check onc-python version/installation.")
     else: sys.exit(f"ERROR: Unexpected AttributeError during import: {ae}")


from . import utils
from . import ui
from .config import PNG_DEFAULT_PARAMS, WAV_DEFAULT_PARAMS, SUPPORTED_EXTENSIONS
from .exceptions import ONCInteractionError, NoDataError, DownloadError

# --- Discovery Functions ---

def find_overlapping_deployments(
    onc_client: ONC,
    start_utc: datetime,
    end_utc: datetime,
    args: Any
) -> Tuple[List[Dict], Dict[str, str]]:
    """Finds hydrophone deployments overlapping with the specified time range."""
    logging.info("Fetching locations...")
    try:
        all_locations = onc_client.getLocations({})
        loc_map = {l["locationCode"]: l["locationName"] for l in all_locations if isinstance(l, dict) and "locationCode" in l}
        if not loc_map: logging.warning("No locations found or parsed.")
        utils.dbg("Location map", loc_map if loc_map else "<empty>", args=args)
    except Exception as e:
        raise ONCInteractionError(f"Failed to get locations: {e}") from e

    logging.info("Finding hydrophone deployments...")
    utcnow = datetime.now(UTC)
    deployments: List[Dict] = []
    skipped_deployments_count = 0
    skipped_devices_count = 0

    try:
        all_hydrophones = onc_client.getDevices({"deviceCategoryCode": "HYDROPHONE"})
        if not all_hydrophones:
            logging.info("No hydrophone devices found.")
            return [], loc_map # Return empty list if no hydrophones
    except Exception as e:
        raise ONCInteractionError(f"Failed to get hydrophone devices: {e}") from e

    for dev in all_hydrophones:
        if not isinstance(dev, dict) or not dev.get("deviceCode"):
            logging.warning("Skipping invalid device entry.")
            skipped_devices_count += 1
            continue
        device_code = dev["deviceCode"]
        utils.dbg(f"Checking deployments for {device_code} ({dev.get('deviceName','?')})", args=args)

        try:
            device_deployments = onc_client.getDeployments({"deviceCode": device_code})
            if not isinstance(device_deployments, list):
                logging.warning(f"Unexpected response type ({type(device_deployments)}) for {device_code} deployments. Skipping device.")
                skipped_devices_count += 1
                continue

            for dep in device_deployments:
                if not isinstance(dep, dict):
                    skipped_deployments_count += 1
                    continue # Skip malformed deployment entries
                try:
                    begin_str = dep.get("begin")
                    if not begin_str:
                        skipped_deployments_count += 1
                        continue # Skip deployment without a start date

                    # Parse deployment dates, assuming UTC if naive
                    b = dtparse.isoparse(begin_str)
                    if b.tzinfo is None: b = b.replace(tzinfo=UTC)
                    else: b = b.astimezone(UTC)

                    e_str = dep.get("end")
                    e = dtparse.isoparse(e_str) if e_str else utcnow # Use current time if no end date
                    if e.tzinfo is None: e = e.replace(tzinfo=UTC)
                    else: e = e.astimezone(UTC)

                    # Check for overlap: (DepStart <= ReqEnd) and (DepEnd >= ReqStart)
                    if b <= end_utc and e >= start_utc:
                        d_info = dev.copy()
                        d_info.update(dep) # Combine device and deployment info
                        deployments.append(d_info)
                        logging.info(f"  -> Found overlap: {device_code} ({b.strftime('%Y-%m-%d')} to {e.strftime('%Y-%m-%d') if e_str else 'now'})")
                    #else:
                    #    utils.dbg(f"  -> No overlap: {device_code} ({b.strftime('%Y-%m-%d')} to {e.strftime('%Y-%m-%d') if e_str else 'now'}) vs Req: ({start_utc.strftime('%Y-%m-%d')} to {end_utc.strftime('%Y-%m-%d')})", args=args)

                except (dtparse.ParserError, ValueError) as date_err:
                    logging.warning(f"Could not parse dates for deployment of {device_code}. Begin: '{dep.get('begin')}', End: '{dep.get('end')}'. Error: {date_err}")
                    skipped_deployments_count += 1
                except Exception as inner_e:
                    logging.warning(f"Error processing deployment entry for {device_code}: {inner_e}")
                    skipped_deployments_count += 1

        except HTTPError as http_err:
            if http_err.response is not None and http_err.response.status_code == 404:
                utils.dbg(f"No deployments found (404) for device {device_code}. Skipping device.", args=args)
            else:
                logging.warning(f"HTTPError getting deployments for {device_code}: {http_err}")
            skipped_devices_count += 1
            continue # Skip to the next device
        except Exception as e:
            logging.warning(f"Unexpected error getting deployments for {device_code}: {e}", exc_info=args.debug)
            skipped_devices_count += 1
            continue # Skip to the next device

    if skipped_deployments_count > 0: logging.info(f"Note: Skipped {skipped_deployments_count} individual deployment entries due to date/parse issues.")
    if skipped_devices_count > 0: logging.info(f"Note: Skipped {skipped_devices_count} devices for which deployments could not be retrieved (e.g., 404 or other errors).")
    if not deployments:
        raise NoDataError("No overlapping hydrophone deployments found for the specified time range.")

    logging.info(f"Found {len(deployments)} overlapping deployments.")
    return deployments, loc_map


def _extract_name_from_citation(citation_string: Optional[str]) -> Optional[str]:
    """Attempts to extract a location name from the citation string."""
    if not citation_string:
        return None

    # Try to find patterns like "... YYYY. [Location Name] Hydrophone Deployed YYYY-MM-DD..."
    # Or "... YYYY. [Location Name] Deployed YYYY-MM-DD..."
    # Make it non-greedy and look for "Deployed YYYY-MM-DD" as an end marker
    match = re.search(r'\.\s*\d{4}\.\s*(.*?)(?:\s+Hydrophone)?\s+Deployed\s+\d{4}-\d{2}-\d{2}', citation_string, re.IGNORECASE)

    if match:
        potential_name = match.group(1).strip()
        # Avoid overly generic terms if possible, though might be hard
        if potential_name and potential_name.lower() not in ["hydrophone", "underwater network"]:
             # Simple cleanup: remove trailing punctuation if any
             potential_name = potential_name.rstrip('.,;:!?)(')
             return potential_name

    # Fallback: Simpler pattern if the above fails - just take text after year dot
    match_simple = re.search(r'\.\s*\d{4}\.\s*(.*)', citation_string)
    if match_simple:
         potential_name = match_simple.group(1).strip()
         # Crude check to remove trailing date/doi parts if they exist
         date_match = re.search(r'\d{4}-\d{2}-\d{2}', potential_name)
         if date_match:
              potential_name = potential_name[:date_match.start()].strip()
         doi_match = re.search(r'https?://doi.org', potential_name, re.IGNORECASE)
         if doi_match:
             potential_name = potential_name[:doi_match.start()].strip()

         # Remove trailing hydrophone/deployment info if present
         potential_name = re.sub(r'\s+Hydrophone\s+Deployed.*$', '', potential_name, flags=re.IGNORECASE).strip()
         potential_name = re.sub(r'\s+Deployed.*$', '', potential_name, flags=re.IGNORECASE).strip()
         potential_name = potential_name.rstrip('.,;:!?)(')

         if potential_name and potential_name.lower() not in ["hydrophone", "underwater network"]:
              return potential_name


    return None # Could not extract a likely name


def select_location_and_devices(
    deployments: List[Dict],
    loc_map: Dict[str, str] # Map from getLocations({})
) -> Tuple[List[Dict], str]:
    """Prompts user to select a parent location, shows all hydrophone codes at that location,
       then prompts for specific hydrophone(s)."""

    # --- Group deployments by PARENT location code ---
    by_parent_loc = defaultdict(list)
    parent_codes_found = set()

    for d in deployments:
        loc_code_from_dep = d.get('locationCode')
        if not loc_code_from_dep:
            continue

        parent_code = loc_code_from_dep
        if '.' in loc_code_from_dep:
            parent_code = loc_code_from_dep.split('.')[0]

        by_parent_loc[parent_code].append(d)
        parent_codes_found.add(parent_code)
    # --- End Grouping ---

    if not by_parent_loc:
        raise NoDataError("No deployments found with processable location codes.")

    sorted_parent_codes = sorted(list(parent_codes_found))

    # --- Build PARENT Location Choices with Associated Device Codes ---
    parent_loc_choices = []
    parent_choice_details = {}

    # Define known generic/undesirable names from loc_map
    # Add more here if you find others
    GENERIC_LOC_MAP_NAMES = {"Hydrophone Array - Box Type", "Underwater Network"}

    print("\n--- Determining Parent Locations and Associated Hydrophones ---")
    for parent_code in sorted_parent_codes:
        # print(f"\nProcessing Parent Code: '{parent_code}'") # Optional debug
        display_name = None
        source_of_name = "Unknown"
        deployments_at_parent = by_parent_loc[parent_code]
        first_deployment = deployments_at_parent[0] if deployments_at_parent else None

        # --- Naming Logic ---
        # 1. Get name from loc_map
        parent_map_name = loc_map.get(parent_code)
        is_generic_from_map = False
        if parent_map_name:
             if parent_map_name in GENERIC_LOC_MAP_NAMES:
                 is_generic_from_map = True
                 # print(f"  -> Name Source: Direct Lookup (loc_map['{parent_code}']) -> '{parent_map_name}' (Generic)") # Optional debug
             else:
                 # Use the map name if it exists and is NOT generic
                 display_name = parent_map_name
                 source_of_name = f"Direct Lookup (loc_map['{parent_code}'])"
                 # print(f"  -> Name Source: {source_of_name} -> '{display_name}'") # Optional debug
        # else:
             # print(f"  -> Name Source: Direct Lookup (loc_map['{parent_code}']) FAILED") # Optional debug


        # 2. Try citation parsing ONLY IF map lookup failed OR gave a generic name.
        if display_name is None: # This means map lookup failed OR was generic
            if first_deployment:
                citation_text = first_deployment.get('citation', {}).get('citation')
                citation_name = _extract_name_from_citation(citation_text)
                if citation_name:
                    display_name = citation_name # Use citation name
                    source_of_name = "Citation Parse"
                    # print(f"  -> Name Source: {source_of_name} -> '{display_name}'") # Optional debug
                # else:
                    # print(f"  -> Name Source: Citation Parse FAILED") # Optional debug

        # 3. Fallback: If we STILL don't have a name...
        if display_name is None:
            # Use the generic map name if we had one originally
            if parent_map_name and is_generic_from_map:
                display_name = parent_map_name
                source_of_name = "Generic Map Fallback"
                # print(f"  -> Name Source: {source_of_name} -> '{display_name}'") # Optional debug
            # Otherwise, use the code itself
            else:
                display_name = parent_code
                source_of_name = "Code Fallback"
                # print(f"  -> Name Source: {source_of_name} -> '{display_name}'") # Optional debug
        # --- End Naming Logic ---


        # --- Get Device List (Codes Only) ---
        device_codes_only = set()
        for dep in deployments_at_parent:
            device_code = dep.get('deviceCode')
            if device_code:
                device_codes_only.add(device_code)

        device_list_str = ""
        if device_codes_only:
            sorted_codes = sorted(list(device_codes_only))
            device_list_str = f" (Hydrophones: {'; '.join(sorted_codes)})"
            # print(f"  -> Devices Found:{device_list_str}") # Optional debug
        # else:
             # print(f"  -> Devices Found: None Listed for parent '{parent_code}'") # Optional debug
        # --- End Get Device List ---

        # Store details
        parent_choice_details[parent_code] = {
            'display_name': display_name,
            'all_deployments': deployments_at_parent
        }

        # Format the choice string
        final_choice_string = f"{display_name} [{parent_code}]{device_list_str}"
        parent_loc_choices.append(final_choice_string)
        # print(f"  => Final Prompt String: \"{final_choice_string}\"") # Optional debug

    print("--- End Determining Parent Locations ---")
    # --- End Building Parent Location Choices ---


    # === First Prompt: Select Parent Location ===
    parent_loc_idx = ui.prompt_pick(parent_loc_choices, "Select Parent Location (showing hydrophone codes)")
    selected_parent_code = sorted_parent_codes[parent_loc_idx]

    selected_parent_details = parent_choice_details.get(selected_parent_code)
    if not selected_parent_details:
        logging.error(f"Internal error: Could not retrieve details for selected parent code '{selected_parent_code}'")
        raise NoDataError(f"Failed to process selection for {selected_parent_code}")

    selected_parent_display_name = selected_parent_details['display_name']
    deployments_for_selected_parent = selected_parent_details['all_deployments']

    # --- Determine unique devices available at the selected parent location ---
    devices_at_selected_parent = {} # {deviceCode: deviceName}
    for dep in deployments_for_selected_parent:
        d_code = dep.get('deviceCode')
        d_name = dep.get('deviceName', 'Unknown Device')
        if d_code and d_code not in devices_at_selected_parent:
             devices_at_selected_parent[d_code] = d_name

    # === Second Prompt: Select Specific Hydrophone(s) ===
    hydrophone_menu = ["ALL Hydrophones at this location"]
    sorted_device_codes = sorted(devices_at_selected_parent.keys())
    hydrophone_menu.extend([
        f"{devices_at_selected_parent[code]} ({code})"
        for code in sorted_device_codes
    ])

    chosen_deps = []
    if len(sorted_device_codes) > 1:
        idx = ui.prompt_pick(hydrophone_menu, f"Select Specific Hydrophone for {selected_parent_display_name} [{selected_parent_code}]")
        if idx == 0:
            chosen_deps = deployments_for_selected_parent
        else:
            selected_device_code = sorted_device_codes[idx - 1]
            chosen_deps = [dep for dep in deployments_for_selected_parent if dep.get('deviceCode') == selected_device_code]
    elif len(sorted_device_codes) == 1:
         selected_device_code = sorted_device_codes[0]
         logging.info(f"Auto-selecting the only available hydrophone: {devices_at_selected_parent[selected_device_code]} ({selected_device_code})")
         chosen_deps = [dep for dep in deployments_for_selected_parent if dep.get('deviceCode') == selected_device_code]
    else:
         logging.warning(f"No specific hydrophone devices identified for {selected_parent_display_name}, proceeding with all {len(deployments_for_selected_parent)} deployments for this parent location.")
         chosen_deps = deployments_for_selected_parent

    # --- Logging and Return ---
    final_device_codes = sorted(list(set(d.get('deviceCode', '?') for d in chosen_deps)))
    logging.info(f"Selected {len(chosen_deps)} deployment(s) for location {selected_parent_display_name} [{selected_parent_code}] / Devices: {', '.join(final_device_codes)}")

    return chosen_deps, selected_parent_code

def select_data_products(
    onc_client: ONC,
    chosen_deps: List[Dict],
    args: Any
) -> Dict[str, Dict]:
    """Gets available products for the first selected device and prompts user for selection."""
    if not chosen_deps:
         raise ValueError("Cannot select products without chosen deployments.")

    first_device_code = chosen_deps[0].get("deviceCode")
    if not first_device_code:
        raise NoDataError("Selected deployment is missing a device code.")

    logging.info(f"Fetching available data products for device {first_device_code}...")
    try:
        prod_opts = onc_client.getDataProducts({"deviceCode": first_device_code})
        if not isinstance(prod_opts, list):
            logging.warning(f"Unexpected response type ({type(prod_opts)}) for getDataProducts. Assuming no products.")
            prod_opts = []
    except Exception as e:
        raise ONCInteractionError(f"Failed to get data products for {first_device_code}: {e}") from e

    utils.dbg("Available products response", prod_opts, args=args)

    # Filter for relevant extensions and group by extension
    ext2opt = defaultdict(list)
    for p in prod_opts:
        if isinstance(p, dict) and p.get('extension') in SUPPORTED_EXTENSIONS:
            ext2opt[p['extension']].append(p)

    if not ext2opt:
         raise NoDataError(f"No supported data products ({', '.join(SUPPORTED_EXTENSIONS)}) found for device {first_device_code}.")

    # Determine which extensions to request based on CLI args or prompts
    wanted_explicit = {"wav": args.wav, "png": args.png, "txt": args.txt}
    cli_wants_any = any(wanted_explicit.values())
    wanted_exts = []

    if cli_wants_any:
        wanted_exts = [k for k, v in wanted_explicit.items() if v and k in ext2opt]
        if not wanted_exts:
             logging.warning(f"CLI flags specified data types ({[k for k,v in wanted_explicit.items() if v]}), but none are available for {first_device_code}.")
             raise NoDataError(f"Requested data types not available for device {first_device_code}.")
    else:
        print("\nSelect data types to download:")
        prompted_exts = []
        available_prompt_exts = [ext for ext in SUPPORTED_EXTENSIONS if ext in ext2opt]
        if not available_prompt_exts:
             raise NoDataError(f"No supported data products available to choose from for device {first_device_code}.")

        for ext in available_prompt_exts:
            try:
                 # Show product name if available
                 prod_name = ext2opt[ext][0].get('dataProductName', '?') if ext2opt[ext] else '?'
                 ans = input(f"Fetch {ext.upper()} ({prod_name})? [y/N] ").lower()
                 if ans == 'y':
                     prompted_exts.append(ext)
            except (EOFError, KeyboardInterrupt):
                 print("\n✖ User interruption during selection.")
                 raise ui.UserAbortError("User aborted during data type selection.")
        wanted_exts = prompted_exts

    if not wanted_exts:
        raise NoDataError("No data types were selected for download.")

    # Choose the first available product for each selected extension
    # (More sophisticated logic could allow choosing between multiple products of the same type if needed)
    chosen_products = {}
    for ext in wanted_exts:
        if ext in ext2opt and ext2opt[ext]:
             chosen_products[ext] = ext2opt[ext][0] # Take the first product matching the extension
             prod = chosen_products[ext]
             logging.info(f"Selected {ext.upper()}: {prod.get('dataProductName','?')} ({prod.get('dataProductCode','?')})")
        else:
            # This case should be rare given the previous checks
             logging.warning(f"Selected extension '{ext}' is somehow not available after filtering. Skipping.")


    if not chosen_products:
         raise NoDataError("No valid data products could be selected based on user choice and availability.")

    return chosen_products


# --- Job Request & Download Functions ---

def request_onc_jobs(
    onc_client: ONC,
    chosen_deps: List[Dict],
    chosen_products: Dict[str, Dict],
    start_utc: datetime,
    end_utc: datetime,
    args: Any
) -> Tuple[List[Tuple[int, str, str]], int]:
    """Requests data product jobs from ONC for selected devices and products."""
    logging.info("Requesting data products...")
    total_bytes_est = 0
    jobs = [] # List of (request_id, device_code, extension)
    restricted_any = False

    for dep in chosen_deps:
        if not isinstance(dep, dict):
            logging.warning("Skipping invalid deployment entry during job request.")
            continue
        device_code = dep.get("deviceCode")
        if not device_code:
            logging.warning(f"Skipping deployment with missing device code: {dep.get('deviceName','?')}")
            continue

        logging.info(f"--- Requesting for Device: {device_code} ({dep.get('deviceName','?')}) ---")
        for ext, prod in chosen_products.items():
            if not isinstance(prod, dict):
                 logging.warning(f"Skipping invalid product entry for extension {ext}.")
                 continue
            data_product_code = prod.get("dataProductCode")
            if not data_product_code:
                 logging.warning(f"Skipping product with missing code for extension {ext}: {prod.get('dataProductName','?')}")
                 continue

            logging.info(f"Requesting {ext.upper()} (Product: {data_product_code})...")
            payload = dict(
                deviceCode=device_code,
                dataProductCode=data_product_code,
                extension=ext,
                dateFrom=utils.iso(start_utc),
                dateTo=utils.iso(end_utc)
            )
            # Add product-specific defaults
            if ext == "png": payload.update(PNG_DEFAULT_PARAMS)
            elif ext == "wav": payload.update(WAV_DEFAULT_PARAMS)

            utils.dbg("Request Payload:", payload, args=args)
            try:
                # Use a timeout for the request itself
                req_info = onc_client.requestDataProduct(payload) # Timeout here applies to the API call, not data generation
                if not isinstance(req_info, dict):
                    logging.error(f"Request Error {ext}/{device_code}: Unexpected response type {type(req_info)}. Response: {req_info}")
                    continue

                utils.dbg(f"Request OK: {ext.upper()}/{device_code}", req_info, args=args)

                # Check for restricted data warnings
                if any("restricted" in str(w).lower() for w in req_info.get("warningMessages", [])):
                    logging.warning(f"⚠ Request {ext.upper()}/{device_code} has RESTRICTED data warning. Skipping this job.")
                    restricted_any = True
                    continue

                # Check for errors in the response
                if req_info.get("errors"):
                    logging.error(f"✖ Request {ext.upper()}/{device_code} failed with errors reported by ONC:")
                    for err in req_info["errors"]:
                         logging.error(f"  - {err.get('errorCode')}: {err.get('errorMessage')}")
                         if 'parameter' in err: logging.error(f"    Parameter: {err['parameter']}")
                    continue # Skip this job

                size_mb = utils.extract_mb(req_info, args.debug)
                total_bytes_est += int(size_mb * 1048576)

                dp_id = req_info.get("dpRequestId")
                if dp_id is None:
                     logging.error(f"Request Error {ext}/{device_code}: Response missing 'dpRequestId'. Response: {req_info}")
                     continue
                try:
                     dp_id_int = int(dp_id)
                except (ValueError, TypeError):
                     logging.error(f"Request Error {ext}/{device_code}: Invalid 'dpRequestId' format ({dp_id}). Response: {req_info}")
                     continue

                logging.info(f"  Request ID: {dp_id_int}, Est. Size: {utils.human_size(int(size_mb*1048576))}")
                jobs.append((dp_id_int, device_code, ext))

            except HTTPError as http_err:
                 logging.error(f"Request HTTP Error {ext}/{device_code}: {http_err}")
                 # Optionally show response content if available and debugging
                 if args.debug and http_err.response is not None:
                     try:
                         logging.debug(f"    Response Status: {http_err.response.status_code}")
                         logging.debug(f"    Response Body: {http_err.response.text}")
                     except Exception: pass # Ignore errors reading response details
            except Exception as e:
                 # Catch other potential exceptions during the request
                 logging.error(f"Request System Error {ext}/{device_code}: {e}", exc_info=args.debug)

    if restricted_any:
        print("\n⚠ NOTE: Some data requests involved restricted data and were skipped.")
    if not jobs:
        raise NoDataError("No data processing jobs were successfully created.")

    return jobs, total_bytes_est


def _attempt_fallback_download(
    request_id: int,
    actual_run_id: int,
    device_c: str, # Keep for context, though run_id is key
    file_ext: str,
    onc_client: ONC,
    args: Any,
    run_info: Optional[Dict] = None
) -> bool:
    """
    Fallback mechanism to download files individually using the actual runId.

    Args:
        request_id: The original data product request ID (dpRequestId).
        actual_run_id: The specific run ID (dpRunId) for this execution.
        device_c: Device code (for logging).
        file_ext: The expected file extension.
        onc_client: Initialized ONC client.
        args: Parsed command-line arguments.
        run_info: Optional dictionary from runDataProduct result.

    Returns:
        True if fallback succeeded (fully or partially), False otherwise.
    """
    logging.warning(f"Attempting fallback download for request {request_id} (using actual runId {actual_run_id})...")

    file_list_infos = []
    fallback_succeeded = False
    file_count = -1 # Initialize file count as unknown

    # --- 1. Determine File Count ---
    # Try getting file count directly from run_info first if available
    if run_info and isinstance(run_info, dict):
        file_count = run_info.get('fileCount', -1)
        if file_count >= 0:
            logging.info(f"Fallback: Using fileCount={file_count} from runDataProduct result.")
        else:
            logging.warning("Fallback: fileCount missing or invalid in run_info. Will attempt to poll/count.")
            file_count = -1 # Reset to ensure polling logic runs

    # If file count is still unknown, poll status and try internal count method
    if file_count < 0:
        initial_wait = 3.0 # Short wait before first status check
        logging.info(f"Fallback: File count unknown. Waiting {initial_wait}s then polling status for request {request_id}...")
        time.sleep(initial_wait)
        onc_status = 'UNKNOWN'

        for attempt in range(args.fallback_retries + 1): # +1 because we wait between retries
            try:
                utils.dbg(f"Fallback attempt {attempt + 1}/{args.fallback_retries}: Checking status for request {request_id}", args=args)
                # Use checkDataProduct on the *request_id* to see overall status
                status_check_result = onc_client.checkDataProduct(request_id)
                utils.dbg(f"Status check response for {request_id}:", status_check_result, args=args)

                status_info = None
                if isinstance(status_check_result, dict):
                    status_info = status_check_result
                elif isinstance(status_check_result, list) and status_check_result and isinstance(status_check_result[0], dict):
                    # Sometimes it returns a list with one status dict
                    status_info = status_check_result[0]
                else:
                    logging.error(f"Fallback: Invalid structure from checkDataProduct for request {request_id}. Type: {type(status_check_result)}")
                    # Wait and retry, maybe it's a transient issue
                    if attempt < args.fallback_retries: time.sleep(args.fallback_wait); continue
                    else: break # Failed after retries

                onc_status = status_info.get('searchHdrStatus', 'UNKNOWN').upper()
                utils.dbg(f"Request {request_id} ONC status: {onc_status}", args=args)

                if onc_status in ['COMPLETE', 'COMPLETED']:
                    logging.info(f"Fallback: Request status is {onc_status}. Determining file count using runId {actual_run_id}...")
                    try:
                        # Use the *actual_run_id* for the count method
                        logging.info(f"Fallback: Using internal _countFilesInProduct for runId {actual_run_id}...")
                        # This is an internal/potentially unstable method, use with caution
                        file_count = onc_client.delivery._countFilesInProduct(actual_run_id)
                        if file_count < 0:
                             logging.warning(f"Fallback: _countFilesInProduct returned {file_count} for runId {actual_run_id}. Assuming count failed.")
                             file_count = -1 # Mark as failed
                        else:
                             logging.info(f"Fallback: _countFilesInProduct determined file count: {file_count}")

                    except AttributeError:
                         logging.error("Fallback: ONC client object missing 'delivery' attribute or '_countFilesInProduct' method. Cannot count files.")
                         file_count = -1
                    except Exception as count_err:
                         logging.error(f"Fallback: Error calling _countFilesInProduct for runId {actual_run_id}: {count_err}", exc_info=args.debug)
                         file_count = -1
                    break # Exit status polling loop once COMPLETE status is reached

                elif onc_status in ['FAILED', 'CANCELLED']:
                    logging.error(f"✖ Fallback cannot proceed: Request {request_id} status is '{onc_status}'.")
                    file_count = -1 # Ensure failure state
                    break # Stop polling

                else: # Still running, queued, etc.
                    utils.dbg(f"Status '{onc_status}'. Waiting {args.fallback_wait}s...", args=args)

            except Exception as e:
                logging.error(f"Error during fallback status poll for request {request_id}: {e}", exc_info=args.debug)
                file_count = -1 # Assume failure on exception
                break # Stop polling

            # Wait before the next attempt
            if attempt < args.fallback_retries:
                time.sleep(args.fallback_wait)
            else:
                logging.error(f"Fallback timed out waiting for request {request_id} to complete (last status: {onc_status}).")
                file_count = -1 # Mark as failed due to timeout

    # --- After Status Check / File Count Determination ---
    if file_count < 0:
        logging.error(f"✖ Fallback failed for request {request_id} (runId {actual_run_id}). Could not determine file count.")
        return False # Cannot proceed without file count

    if file_count == 0:
        logging.info(f"✔ Fallback determined 0 data files for request {request_id} (runId {actual_run_id}). Assuming success.")
        return True # Nothing to download

    # --- 2. Generate File List ---
    logging.info(f"Fallback: Generating file list for {file_count} files (using runId {actual_run_id})...")
    indexes_to_try = [str(i) for i in range(1, file_count + 1)] # Data files are typically indexed 1 to N
    try:
        # Need base URL and token for _DataProductFile internal class
        # Accessing protected members (_baseUrl, _token) is not ideal but necessary here
        base_url = onc_client.baseUrl
        token = onc_client.token
        if not base_url or not token:
             logging.error("Fallback Error: Cannot get baseUrl or token from ONC client.")
             return False

        # Instantiate _DataProductFile for each index to get its metadata
        # This involves API calls for each file's info!
        file_list_infos = [_DataProductFile(actual_run_id, index, base_url, token).getInfo() for index in indexes_to_try]

        # Validate generated list
        valid_infos = [info for info in file_list_infos if isinstance(info, dict) and info.get('index')]
        if len(valid_infos) != file_count:
             logging.warning(f"Fallback: Generated info for {len(valid_infos)} files, but expected {file_count}. Some file info might be missing.")
             # Proceed with what we have, but log the discrepancy

        if not valid_infos: # Check if we got *any* valid info
            logging.error(f"Fallback: Failed to generate any valid file info objects for runId {actual_run_id}.")
            return False

        file_list_infos = valid_infos # Use only the valid ones

    except AttributeError as ae:
        logging.error(f"Fallback Error: Missing attributes needed for _DataProductFile (check onc-python version?). Error: {ae}", exc_info=args.debug)
        return False
    except Exception as gen_err:
        logging.error(f"Fallback Error generating file info list for runId {actual_run_id}: {gen_err}", exc_info=args.debug)
        return False

    # --- 3. Individual File Download ---
    logging.info(f"Fallback: Attempting download for {len(file_list_infos)} file(s) individually (runId {actual_run_id})...")
    files_downloaded_count = 0
    files_failed_count = 0
    files_skipped_count = 0 # Count files skipped because they already exist

    for f_info in file_list_infos:
        index_to_download = f_info.get('index')
        if not index_to_download:
            logging.warning(f"Fallback: Skipping entry with missing index in file info: {f_info}")
            files_failed_count += 1
            continue

        utils.dbg(f"Fallback: Downloading runId={actual_run_id}, index={index_to_download}", args=args)
        # Construct a potential filename for logging before download attempt
        potential_filename = f"file_{actual_run_id}_{index_to_download}.{file_ext}"
        actual_filename = potential_filename # Default name if download fails early

        try:
            # Small delay between individual file requests might help avoid rate limits
            time.sleep(0.5) # Shorter delay for individual files

            # Use _DataProductFile internal class to download this specific file index
            downloader = _DataProductFile(actual_run_id, index_to_download, onc_client.baseUrl, onc_client.token)

            # Use download method of the internal class
            # Note: This download method might behave slightly differently than onc.downloadDataProduct
            # It takes different parameters (e.g., pollPeriod might not be used same way)
            # We set overwrite=True to match the main download logic intent
            status_code = downloader.download(
                timeout=onc_client.timeout,      # Use main client timeout
                pollPeriod=1.0,                # Internal download handles its own retries/logic
                outPath=onc_client.outPath,      # Use main client output path
                maxRetries=3,                    # Use a few retries for individual files
                overwrite=True                   # Overwrite if exists locally
            )

            # Get info again *after* download attempt to get final status and filename
            downloaded_info = downloader.getInfo()
            actual_filename = downloaded_info.get('file') or actual_filename # Use actual name if available

            # Interpret the status code returned by the internal download method
            if status_code == 200:
                size = downloaded_info.get('size', 0)
                logging.info(f"  -> DL OK: {actual_filename} (Idx:{index_to_download}, Size:{utils.human_size(size)})")
                files_downloaded_count += 1
            elif status_code == 777: # Internal code for "already exists and overwrite=False" (shouldn't happen with overwrite=True)
                logging.info(f"  -> Skip: {actual_filename} (Idx:{index_to_download}) - Exists (unexpected with overwrite=True).")
                files_skipped_count += 1
             # Add check for FileExistsError being caught separately below, which is more reliable for skipping
            elif status_code == 204: # No content
                logging.warning(f"  -> Fail: No content (204) for index {index_to_download}, runId {actual_run_id}.")
                files_failed_count += 1
            elif status_code == 410: # Gone (file expired on server)
                logging.warning(f"  -> Fail: File Gone (410) for index {index_to_download}, runId {actual_run_id}.")
                files_failed_count += 1
            elif status_code == 404: # Not Found (index invalid?)
                 logging.warning(f"  -> Fail: Not Found (404) for index {index_to_download}, runId {actual_run_id}.")
                 files_failed_count += 1
            else:
                # General failure
                fail_status_msg = downloaded_info.get('status', 'Unknown Status')
                logging.error(f"  -> Fail: DL Index {index_to_download}, runId {actual_run_id}. Status Code: {status_code} ({fail_status_msg})")
                utils.dbg("Failed download info:", downloaded_info, args=args)
                files_failed_count += 1

        except FileExistsError:
            # This is the expected way to detect existing files when overwrite=True isn't fully handled internally or fails
             logging.info(f"  -> Skip: {actual_filename} (Idx:{index_to_download}) - File already exists locally.")
             files_skipped_count += 1
        except Exception as download_err:
            logging.error(f"  -> Fail: System error downloading index {index_to_download}, runId {actual_run_id}: {download_err}", exc_info=args.debug)
            files_failed_count += 1

    # --- 4. Fallback Summary ---
    total_processed = files_downloaded_count + files_skipped_count + files_failed_count
    expected_total = len(file_list_infos) # Use the count of infos we attempted to download

    if files_failed_count == 0:
        logging.info(f"✔ Fallback OK for request {request_id} (runId {actual_run_id}). Got {files_downloaded_count} new, skipped {files_skipped_count} existing files.")
        fallback_succeeded = True
    elif files_downloaded_count > 0 or files_skipped_count > 0:
        logging.warning(f"⚠ Fallback PARTIAL for req {request_id} (runId {actual_run_id}): {files_failed_count} error(s). Got {files_downloaded_count} new, skipped {files_skipped_count} existing.")
        fallback_succeeded = True # Considered success if *any* files were obtained/skipped correctly
    else:
        logging.error(f"✖ Fallback FAILED for req {request_id} (runId {actual_run_id}). Failed all {files_failed_count} attempted downloads.")
        fallback_succeeded = False

    if total_processed != expected_total:
         logging.warning(f"Fallback Discrepancy: Processed {total_processed} files, but expected to process {expected_total} based on generated info list.")


    return fallback_succeeded


def process_download_jobs(
    jobs: List[Tuple[int, str, str]],
    onc_client: ONC,
    output_path: Path, # Passed explicitly
    args: Any
) -> bool:
    """Runs, monitors, and downloads data for the requested jobs."""
    logging.info(f"Starting downloads to: {onc_client.outPath}") # outPath is configured in the client
    print("\nStarting download process...")
    all_jobs_overall_success = True
    job_statuses = {} # Store final status per job request_id

    for request_id, device_code, ext in jobs:
        print(f"\n--- Processing Request ID: {request_id} ({device_code} {ext.upper()}) ---")
        job_status_key = f"Req_{request_id}_{device_code}_{ext}"
        job_succeeded = False  # Tracks success of this specific job (run + download/fallback)
        trigger_fallback_flag = False
        run_info: Optional[Dict] = None
        run_succeeded = False
        job_status_final = 'UNKNOWN'
        actual_run_id: Optional[int] = None # Store the actual run ID

        try:
            # === Step 1: Run the data product request and wait for completion ===
            logging.info(f"Running request ID {request_id} and waiting for ONC processing...")
            # waitComplete=True asks the onc-python library to poll until the job is 'complete' or 'failed' on ONC's side.
            # The timeout applies to the *polling* duration within onc-python.
            # A very long-running job on ONC's side might still exceed this timeout.
            run_info = onc_client.runDataProduct(dpRequestId=request_id, waitComplete=True)
            utils.dbg(f"runDataProduct result for {request_id}:", run_info, args=args)

            # === Step 1b: Validate run_info, get actual run ID, and confirm status ===
            if isinstance(run_info, dict) and isinstance(run_info.get('runIds'), list) and run_info['runIds']:
                # Usually, there's one runId per request run. Take the first.
                actual_run_id = run_info['runIds'][0]
                utils.dbg(f"Obtained actual runId: {actual_run_id} for request {request_id}", args=args)

                # Double-check the status using checkDataProduct after runDataProduct returns
                try:
                    status_check_result = onc_client.checkDataProduct(request_id)
                    utils.dbg(f"Status check result after run for {request_id}:", status_check_result, args=args)
                    status_info = None
                    if isinstance(status_check_result, dict):
                        status_info = status_check_result
                    elif isinstance(status_check_result, list) and status_check_result and isinstance(status_check_result[0], dict):
                        status_info = status_check_result[0]
                    else:
                        logging.warning(f"⚠ Status check for {request_id} returned unexpected structure. Assuming run failed.")

                    if status_info:
                        job_status_final = status_info.get('searchHdrStatus', 'UNKNOWN').upper()
                        utils.dbg(f"Parsed final status for {request_id}: {job_status_final}", args=args)
                        if job_status_final in ['COMPLETE', 'COMPLETED']:
                            logging.info(f"Request ID {request_id} processing completed successfully on ONC server.")
                            run_succeeded = True
                            # Check file count reported by runDataProduct
                            file_count = run_info.get('fileCount', -1)
                            if file_count == 0:
                                logging.info(f"  ONC reported 0 files generated for request {request_id}. Download step skipped.")
                                job_succeeded = True # 0 files is a successful outcome
                            elif file_count < 0:
                                logging.warning(f"  ONC reported an invalid file count ({file_count}) for request {request_id}. Proceeding to download attempt.")
                            else:
                                logging.info(f"  ONC reported {file_count} files generated for request {request_id}.")
                        elif job_status_final in ['FAILED', 'CANCELLED']:
                             logging.error(f"✖ Request ID {request_id} processing failed on ONC server (Status: {job_status_final}).")
                             run_succeeded = False
                             # Optionally log error details if present in status_info
                             if status_info.get('errors'):
                                 for err in status_info['errors']:
                                     logging.error(f"    - ONC Error: {err.get('errorMessage', 'Unknown error')}")
                        else:
                             logging.warning(f"⚠ Request ID {request_id} has unexpected final status '{job_status_final}' after run completion. Assuming failure.")
                             run_succeeded = False

                except Exception as status_err:
                    logging.warning(f"⚠ Error checking status after run for request {request_id}: {status_err}. Assuming run failed.")
                    run_succeeded = False
            else:
                logging.error(f"✖ Failed to run or get valid run info for request {request_id}. Result: {run_info}")
                run_succeeded = False
                # Try to get a final status anyway if possible, for logging
                try:
                    status_check_result = onc_client.checkDataProduct(request_id)
                    if isinstance(status_check_result, dict): job_status_final = status_check_result.get('searchHdrStatus', 'UNKNOWN').upper()
                    elif isinstance(status_check_result, list) and status_check_result: job_status_final = status_check_result[0].get('searchHdrStatus', 'UNKNOWN').upper()
                except Exception: pass


            # === Step 2: Attempt Download if Run Succeeded and Files Expected ===
            if run_succeeded and not job_succeeded: # Only download if run ok and not already marked success (e.g., 0 files)
                if actual_run_id is None:
                     logging.error(f"✖ Cannot download for request {request_id}: Actual run ID is unknown.")
                     trigger_fallback_flag = False # Cannot fallback without run ID
                else:
                    # Short wait before download - sometimes ONC needs a moment for files to be ready after 'COMPLETE' status
                    wait_before_download = 5 # seconds
                    logging.info(f"Waiting {wait_before_download}s before download attempt for runId {actual_run_id}...")
                    time.sleep(wait_before_download)

                    logging.info(f"Attempting standard download using runId {actual_run_id}...")
                    download_args = dict(
                        runId=actual_run_id,
                        maxRetries=3,               # Retries for the download process itself
                        downloadResultsOnly=False,
                        includeMetadataFile=False,  # Usually false for hydrophone data files
                        overwrite=True              # Overwrite existing files
                    )
                    utils.dbg("Args for onc.downloadDataProduct:", download_args, args=args)

                    try:
                        # This call downloads all files associated with the runId
                        download_result = onc_client.downloadDataProduct(**download_args)
                        utils.dbg(f"downloadDataProduct result for runId {actual_run_id}:", download_result, args=args)

                        # Process the download result (usually a list of dicts, one per file)
                                                # Process the download result (usually a list of dicts, one per file)
                        if isinstance(download_result, list):
                            if not download_result:
                                # Empty list can mean 0 files (consistent with run_info) or a download issue
                                file_count = run_info.get('fileCount', -1) if run_info else -1
                                if file_count == 0 :
                                    logging.info(f"✔ Download returned empty list, consistent with 0 files reported by ONC.")
                                    job_succeeded = True
                                else:
                                    logging.warning(f"⚠ Download returned empty list, but {file_count if file_count > 0 else 'non-zero'} files were expected. Triggering fallback.")
                                    trigger_fallback_flag = True # Fallback might find files the main download missed
                            else:
                                # Analyze the list of file download statuses more flexibly
                                succ = []
                                errs = []
                                skip = []
                                unknown = []

                                for item in download_result:
                                    if not isinstance(item, dict):
                                        unknown.append(item)
                                        continue # Skip non-dict items

                                    status = str(item.get('status', '')).lower() # Get status, default '', lower case
                                    downloaded = item.get('downloaded', False) # Check if downloaded flag is True

                                    if 'error' in status:
                                        errs.append(item)
                                    elif status == 'skipped':
                                        skip.append(item)
                                    # Consider it success if status is 'complete' OR if the 'downloaded' flag is explicitly True
                                    elif status == 'complete' or downloaded is True:
                                         # Double check it wasn't actually an error disguised as downloaded=True (unlikely but possible)
                                         if 'error' not in status:
                                             succ.append(item)
                                         else: # If 'error' is in status despite downloaded=True, treat as error
                                             errs.append(item)
                                    else:
                                        unknown.append(item) # Add to unknown if status isn't recognized

                                ns, ne, nk, nu = len(succ), len(errs), len(skip), len(unknown)
                                nt = len(download_result) # Total items reported by download function

                                if ne > 0:
                                    logging.error(f"✖ Download for runId {actual_run_id} encountered {ne} error(s). Successful: {ns}, Skipped: {nk}, Unknown: {nu}. Triggering fallback.")
                                    utils.dbg("Download errors:", errs, args=args)
                                    utils.dbg("Unknown status items:", unknown, args=args)
                                    trigger_fallback_flag = True # Try fallback to get missing/failed files
                                elif ns > 0:
                                    logging.info(f"✔ Download successful for {ns} file(s) (runId {actual_run_id}). Skipped {nk} existing file(s). {nu} items had unclear status.")
                                    if nu > 0: # Log if some items were unclear but others succeeded
                                         utils.dbg("Unknown status items (but others succeeded):", unknown, args=args)
                                    job_succeeded = True
                                elif nk == nt and nt > 0: # All files were skipped (already existed)
                                    logging.info(f"✔ Download skipped all {nk} file(s) for runId {actual_run_id} (already exist locally).")
                                    job_succeeded = True
                                elif nu > 0 and ns == 0 and ne == 0 and nk == 0: # Only unknown status items returned
                                     logging.warning(f"⚠ Download for runId {actual_run_id} reported {nt} items, but none had clear success/error/skipped status. Triggering fallback.")
                                     utils.dbg("Unknown status items:", unknown, args=args)
                                     trigger_fallback_flag = True
                                else: # Other combinations (e.g., only skipped and unknown)
                                     logging.warning(f"⚠ Download for runId {actual_run_id} finished with mixed/unclear status (S:{ns}/E:{ne}/K:{nk}/U:{nu}). Triggering fallback.")
                                     utils.dbg("Download results (mixed/unclear):", download_result, args=args)
                                     trigger_fallback_flag = True

                        else: # Unexpected result type from downloadDataProduct
                            logging.warning(f"⚠ Download for runId {actual_run_id} returned unexpected type ({type(download_result)}). Triggering fallback.")
                            trigger_fallback_flag = True

                    except Exception as dl_err:
                         logging.error(f"✖ Error during download attempt for runId {actual_run_id}: {dl_err}", exc_info=args.debug)
                         trigger_fallback_flag = True # Attempt fallback if standard download crashes

            elif not run_succeeded and not job_succeeded:
                 # Log why download is skipped if the run failed
                 logging.info(f"Skipping download for request {request_id} because the ONC processing job failed (Status: {job_status_final}).")


            # === Step 3: Trigger Fallback if Necessary ===
            if trigger_fallback_flag and not job_succeeded:
                if actual_run_id is None:
                    logging.error(f"✖ Cannot attempt fallback for request {request_id}: Actual run ID is unknown.")
                else:
                    # Before fallback, double-check the *final* status is still COMPLETE.
                    # A transient download error shouldn't trigger fallback if the job ultimately failed.
                    final_status_fb = 'UNKNOWN'
                    try:
                        fb_stat_res = onc_client.checkDataProduct(request_id)
                        if isinstance(fb_stat_res, dict): final_status_fb = fb_stat_res.get('searchHdrStatus','UNKNOWN').upper()
                        elif isinstance(fb_stat_res, list) and fb_stat_res: final_status_fb = fb_stat_res[0].get('searchHdrStatus','UNKNOWN').upper()
                        else: final_status_fb = 'ERROR_CHECKING'
                    except Exception as fb_stat_e:
                         logging.warning(f"Could not re-check status before fallback for {request_id}: {fb_stat_e}")
                         final_status_fb = 'ERROR_CHECKING' # Proceed with caution

                    if final_status_fb in ['COMPLETE', 'COMPLETED']:
                        logging.info(f"Proceeding with fallback download for request {request_id} (using runId {actual_run_id}).")
                        # Pass actual_run_id, onc_client, args, and potentially run_info
                        job_succeeded = _attempt_fallback_download(
                            request_id, actual_run_id, device_code, ext, onc_client, args, run_info=run_info
                        )
                        if job_succeeded:
                             logging.info(f"Fallback download appears successful for request {request_id}.")
                        else:
                             logging.error(f"Fallback download failed for request {request_id}.")

                    elif final_status_fb == 'ERROR_CHECKING':
                         logging.warning(f"Could not confirm final status for {request_id} is COMPLETE. Skipping fallback.")
                    else:
                         logging.warning(f"Skipping fallback for request {request_id}; final ONC status is '{final_status_fb}', not COMPLETE.")

        except ONCInteractionError as onc_err:
            logging.error(f"ONC API error during processing of request {request_id}: {onc_err}")
            job_succeeded = False
        except DownloadError as dl_err: # Catch specific download errors if defined
             logging.error(f"Download error for request {request_id}: {dl_err}")
             job_succeeded = False
        except Exception as job_err:
            # Catch unexpected errors during the loop for this job
            logging.error(f"Unexpected error processing request {request_id}: {job_err}", exc_info=True) # Show traceback for unexpected
            job_succeeded = False

        # --- Final Status Logging for this Job ---
        if job_succeeded:
            print(f"✅ Successfully processed Request ID: {request_id}")
            job_statuses[job_status_key] = "Success"
        else:
            print(f"❌ Failed to retrieve data for Request ID: {request_id} (Final ONC Status: {job_status_final})")
            job_statuses[job_status_key] = f"Failed (ONC Status: {job_status_final})"
            all_jobs_overall_success = False # Mark overall failure if any job fails

        # Optional: List directory contents after each job if debugging network issues
        if args.debug_net:
            print(f"--- Contents of '{output_path}' after processing {request_id} ---")
            utils.list_tree(output_path, args=args)
            print("-" * (58 + len(str(request_id))))

    return all_jobs_overall_success, job_statuses