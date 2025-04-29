import logging
import re
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import pprint
import requests
import zipfile
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
    """Gets available products/formats and prompts user for selection."""
    if not chosen_deps:
        raise ValueError("Cannot select products without chosen deployments.")
    
    # In archive mode, skip product selection
    if args.archive:
        return {}
        
    first_deployment = chosen_deps[0]
    first_device_code = first_deployment.get("deviceCode")
    if not first_device_code:
        raise NoDataError("Selected deployment is missing a device code.")

    logging.info(f"Fetching available data products/formats for device {first_device_code}...")
    available_data_products = {}  # Store actual data products (like PNG, TXT)
    flac_available = False  # Flag if FLAC archive likely available

    try:
        # Get actual data products first (PNG, TXT)
        prod_opts = onc_client.getDataProducts({"deviceCode": first_device_code})
        if not isinstance(prod_opts, list): prod_opts = []
        utils.dbg("Available data products response", prod_opts, args=args)

        ext2opt = defaultdict(list)
        for p in prod_opts:
            # Only check for PNG/TXT here as data products
            if isinstance(p, dict) and p.get('extension') in ("png", "txt"):
                ext2opt[p['extension']].append(p)
        available_data_products = ext2opt

        # Check separately if FLAC archive files likely exist (more robust than checking product list)
        # We can do a quick, small getArchivefile check or just assume it's possible if it's a hydrophone
        # For simplicity, let's assume FLAC is *potentially* available if it's a hydrophone
        # A more robust check would involve trying a getArchivefile with rowLimit=1
        first_dev_cat = first_deployment.get("deviceCategoryCode", "").upper()
        if first_dev_cat == "HYDROPHONE":
            logging.info("Device is a hydrophone, assuming FLAC archive files may be available.")
            flac_available = True

    except Exception as e: raise ONCInteractionError(f"Failed to get data products for {first_device_code}: {e}") from e

    if not available_data_products and not flac_available:
         raise NoDataError(f"No supported data products (PNG, TXT) or FLAC audio found/assumed for device {first_device_code}.")

    # Determine which extensions to request based on CLI args or prompts
    # --- Update to use args.flac ---
    wanted_explicit = {"flac": args.flac, "png": args.png, "txt": args.txt}
    cli_wants_any = any(wanted_explicit.values())
    wanted_exts = []

    if cli_wants_any:
        temp_wanted = []
        if wanted_explicit["flac"] and flac_available: temp_wanted.append("flac")
        if wanted_explicit["png"] and "png" in available_data_products: temp_wanted.append("png")
        if wanted_explicit["txt"] and "txt" in available_data_products: temp_wanted.append("txt")
        wanted_exts = temp_wanted
        if not wanted_exts:
             requested_types = [k for k,v in wanted_explicit.items() if v]
             logging.warning(f"CLI flags specified {requested_types}, but none are available/assumed for {first_device_code}.")
             raise NoDataError(f"Requested data types not available for device {first_device_code}.")
    else:
        print("\nSelect data types to download:")
        prompted_exts = []
        # --- Prompt for FLAC ---
        if flac_available:
            try:
                ans = input(f"Fetch FLAC (Archived Audio)? [y/N] ").lower()
                if ans == 'y': prompted_exts.append("flac")
            except (EOFError, KeyboardInterrupt): raise ui.UserAbortError("User aborted.")
        # --- End FLAC Prompt ---
        for ext in ("png", "txt"): # Prompt for PNG/TXT if available
            if ext in available_data_products:
                try:
                    prod_name = available_data_products[ext][0].get('dataProductName', '?') if available_data_products[ext] else '?'
                    ans = input(f"Fetch {ext.upper()} ({prod_name})? [y/N] ").lower()
                    if ans == 'y': prompted_exts.append(ext)
                except (EOFError, KeyboardInterrupt): raise ui.UserAbortError("User aborted.")
        wanted_exts = prompted_exts

    if not wanted_exts: raise NoDataError("No data types were selected.")

    # Prepare the final dictionary - FLAC needs a placeholder 'product' dict
    chosen_products = {}
    if "flac" in wanted_exts:
         chosen_products["flac"] = {"dataProductCode": "ARCHIVE_FLAC", "extension": "flac", "dataProductName": "Archived FLAC Audio"}
         logging.info(f"Selected FLAC: Archived FLAC Audio")
    if "png" in wanted_exts:
         chosen_products["png"] = available_data_products["png"][0]
         logging.info(f"Selected PNG: {chosen_products['png'].get('dataProductName','?')} ({chosen_products['png'].get('dataProductCode','?')})")
    if "txt" in wanted_exts:
         chosen_products["txt"] = available_data_products["txt"][0]
         logging.info(f"Selected TXT: {chosen_products['txt'].get('dataProductName','?')} ({chosen_products['txt'].get('dataProductCode','?')})")

    if not chosen_products: raise NoDataError("No valid products/formats could be selected.")
    return chosen_products


# --- Job Request & Download Functions ---

def request_onc_jobs(
    onc_client: ONC,
    chosen_deps: List[Dict],
    chosen_products: Dict[str, Dict],
    start_utc: datetime,
    end_utc: datetime,
    args: Any
) -> Tuple[List[Tuple[str, Any, str, str]], int]:
    """Requests data product or archive jobs from ONC."""
    logging.info("Requesting data products or preparing archive downloads...")
    total_bytes_est = 0
    jobs = []

    for dep in chosen_deps:
        device_code = dep.get("deviceCode")
        if not device_code:
            logging.warning(f"Skipping deployment with missing device code: {dep.get('deviceName','?')}")
            continue

        logging.info(f"--- Preparing requests for Device: {device_code} ({dep.get('deviceName','?')}) ---")
        
        if args.archive or args.test:
            # In archive/test mode, first list all available files
            logging.info(f"Listing all available archive files...")
            archive_filters = dict(
                deviceCode=device_code,
                dateFrom=utils.iso(start_utc),
                dateTo=utils.iso(end_utc),
                returnOptions='all'  # Request detailed file information
            )
            utils.dbg("Archive Request Filters:", archive_filters, args=args)
            
            try:
                list_result = onc_client.getArchivefile(filters=archive_filters, allPages=True)
                potential_files = list_result.get("files", [])
                files_found = len(potential_files)
                logging.info(f"Found {files_found} {'file' if files_found == 1 else 'files'}.")

                if files_found > 0:
                    # Group files by extension
                    files_by_ext = {}
                    for file_info in potential_files:
                        if not isinstance(file_info, dict):
                            continue
                        filename = file_info.get('filename')
                        if not filename:
                            continue
                            
                        # Extract extension
                        ext = filename.split('.')[-1].lower() if '.' in filename else 'unknown'
                        
                        # Skip thumbnail and small versions of PNG files
                        if ext == 'png' and ('-small.png' in filename or '-thumb.png' in filename):
                            continue
                            
                        if ext not in files_by_ext:
                            files_by_ext[ext] = []
                        files_by_ext[ext].append(file_info)
                    
                    # Print files grouped by extension
                    total_size = 0
                    ext_sizes = {}  # Store sizes by extension
                    for ext in sorted(files_by_ext.keys()):
                        files = files_by_ext[ext]
                        print(f"\n{ext.upper()} Files ({len(files)} {'file' if len(files) == 1 else 'files'} found):")
                        print("-" * 100)
                        ext_size = 0
                        
                        # Sort files by filename within each extension group
                        sorted_files = sorted(files, key=lambda x: x.get('filename', ''))
                        
                        # Get size for all files in this extension group
                        for i, file_info in enumerate(sorted_files, 1):
                            filename = file_info.get('filename', '')
                            file_size = file_info.get('uncompressedFileSize', file_info.get('fileSize', 0))
                            
                            ext_size += file_size
                            if i <= 3:  # Only show first 3 files
                                print(f"{i:3d}. {filename:<75} {utils.human_size(file_size):>10}")
                        
                        if len(files) > 3:
                            print(f"    ... and {len(files) - 3} more files ...")
                        
                        total_size += ext_size
                        ext_sizes[ext] = ext_size  # Store size for this extension
                        print(f"Total {ext.upper()} size: {utils.human_size(ext_size)}")
                    
                    print("\n" + "-" * 100)
                    print(f"Total size of all files: {utils.human_size(total_size)}")
                    print("-" * 100)
                    
                    # In test mode, we're done after listing
                    if args.test:
                        jobs.append(('archive', archive_filters, device_code, 'all'))
                        continue
                    
                    # In archive mode, prompt user for which types to download
                    print("\nSelect file types to download:")
                    wanted_exts = []
                    for ext in sorted(files_by_ext.keys()):
                        try:
                            ext_files = files_by_ext[ext]
                            ext_size = ext_sizes[ext]  # Use stored size
                            ans = input(f"Download {ext.upper()} files? ({len(ext_files)} files, {utils.human_size(ext_size)}) [y/N] ").lower()
                            if ans == 'y':
                                wanted_exts.append(ext)
                        except (EOFError, KeyboardInterrupt):
                            raise ui.UserAbortError("User aborted.")
                    
                    if not wanted_exts:
                        logging.warning("No file types selected for download.")
                        continue
                    
                    # Create archive jobs for selected extensions
                    for ext in wanted_exts:
                        archive_filters_ext = archive_filters.copy()
                        archive_filters_ext['extension'] = ext
                        jobs.append(('archive', archive_filters_ext, device_code, ext))
                        # Add to total size estimate using stored size for this extension
                        total_bytes_est += ext_sizes[ext]
                
                else:
                    logging.info("No archive files found for the specified time range.")
                    
            except Exception as e:
                logging.error(f"Error listing archive files: {e}", exc_info=args.debug)
                raise ONCInteractionError(f"Failed to list archive files: {e}")
                
        else:
            # Normal data product mode - process each requested extension
            for ext, prod in chosen_products.items():
                logging.info(f"Preparing ARCHIVE request for {ext.upper()}...")
                archive_filters = dict(
                    deviceCode=device_code,
                    dateFrom=utils.iso(start_utc),
                    dateTo=utils.iso(end_utc),
                    extension=ext
                )
                utils.dbg("Archive Request Filters:", archive_filters, args=args)
                jobs.append(('archive', archive_filters, device_code, ext))
                
                # Size estimation as before
                if ext == 'flac':
                    duration_hours = (end_utc - start_utc).total_seconds() / 3600
                    rough_flac_est_mb = duration_hours * 6 * 100
                    total_bytes_est += int(rough_flac_est_mb * 1048576)
                    logging.info(f"  Archive request prepared. Rough Est: {utils.human_size(int(rough_flac_est_mb*1048576))}")
                elif ext == 'png':
                    duration_hours = (end_utc - start_utc).total_seconds() / 3600
                    rough_png_est_mb = duration_hours * 0.5
                    total_bytes_est += int(rough_png_est_mb * 1048576)
                    logging.info(f"  Archive request prepared. Rough Est: {utils.human_size(int(rough_png_est_mb*1048576))}")
                elif ext == 'txt':
                    duration_hours = (end_utc - start_utc).total_seconds() / 3600
                    rough_txt_est_mb = duration_hours * 0.1
                    total_bytes_est += int(rough_txt_est_mb * 1048576)
                    logging.info(f"  Archive request prepared. Rough Est: {utils.human_size(int(rough_txt_est_mb*1048576))}")

    if not jobs:
        raise NoDataError("No archive requests or data product jobs could be prepared.")

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
    jobs: List[Tuple[str, Any, str, str]], # Expect ('type', id_or_filters_or_params, device_code, ext)
    onc_client: ONC,
    output_path: Path,
    args: Any
) -> Tuple[bool, Dict[str, Dict]]: # Return detailed job_statuses dict
    """Runs data product jobs, HDP orders, OR downloads archive files."""
    logging.info(f"Starting processing for {len(jobs)} job(s)/order(s)/archive request(s)...")
    print("\nStarting download process...")
    all_jobs_overall_success = True
    job_statuses: Dict[str, Dict[str, Any]] = {}

    for job_type, request_info, device_code, ext in jobs:
        status_info: Dict[str, Any] = {'status': 'Unknown', 'reason': '', 'details': {}}
        job_succeeded = False # Default to not succeeded

        # === Handle Archive File Downloads (e.g., if WAV fallback to archive is needed) ===
        if job_type == 'archive':
            job_status_key = f"Archive_{device_code}_{ext}"
            print(f"\n--- Processing {job_status_key} ---")
            archive_filters = request_info
            files_found = 0; files_existing = 0; files_skipped = 0
            files_downloaded = 0; files_failed = 0; listing_failed = False
            try:
                # 1. List files
                logging.info(f"Listing potential archive files...")
                try: 
                    list_result = onc_client.getArchivefile(filters=archive_filters, allPages=True)
                    potential_files = list_result.get("files", [])
                    
                    # Filter out -small and -thumb PNG files before counting
                    if ext == 'png':
                        potential_files = [f for f in potential_files if isinstance(f, dict) and 
                                        f.get('filename') and 
                                        not ('-small.png' in f['filename'].lower()) and 
                                        not ('-thumb.png' in f['filename'].lower())]
                    
                    files_found = len(potential_files)
                    logging.info(f"Found {files_found} {'file' if files_found == 1 else 'files'}.")

                    # In test mode, print detailed file information
                    if args.test and files_found > 0:
                        print("\nAvailable files:")
                        print("-" * 100)
                        
                        # Group files by extension
                        files_by_ext = {}
                        for file_info in potential_files:
                            if not isinstance(file_info, dict):
                                continue
                            filename = file_info.get('filename')
                            if not filename:
                                continue
                                
                            # Extract extension
                            ext = filename.split('.')[-1].lower() if '.' in filename else 'unknown'
                            
                            # Skip thumbnail and small versions of PNG files
                            if ext == 'png' and ('-small.png' in filename.lower() or '-thumb.png' in filename.lower()):
                                continue
                                
                            if ext not in files_by_ext:
                                files_by_ext[ext] = []
                            files_by_ext[ext].append(file_info)
                        
                        # Print files grouped by extension
                        total_size = 0
                        for ext in sorted(files_by_ext.keys()):
                            files = files_by_ext[ext]
                            print(f"\n{ext.upper()} Files ({len(files)} {'file' if len(files) == 1 else 'files'} found):")
                            print("-" * 100)
                            ext_size = 0
                            
                            # Sort files by filename within each extension group
                            sorted_files = sorted(files, key=lambda x: x.get('filename', ''))
                            
                            # Get size for all files in this extension group
                            for i, file_info in enumerate(sorted_files, 1):
                                filename = file_info.get('filename', '')
                                file_size = file_info.get('uncompressedFileSize', file_info.get('fileSize', 0))
                                
                                ext_size += file_size
                                if i <= 3:  # Only show first 3 files
                                    print(f"{i:3d}. {filename:<75} {utils.human_size(file_size):>10}")
                        
                            if len(files) > 3:
                                print(f"    ... and {len(files) - 3} more files ...")
                            
                            total_size += ext_size
                            print(f"Total {ext.upper()} size: {utils.human_size(ext_size)}")
                        
                        # Set success for test mode since we listed files
                        job_succeeded = True
                        status_info['status'] = 'Success'
                        status_info['reason'] = 'Files Listed (Test Mode)'
                        status_info['details']['total_size'] = total_size
                        continue # Skip to next job in test mode

                except requests.exceptions.HTTPError as http_err: 
                    logging.error(f"✖ HTTP Error listing archive files: {http_err}")
                    listing_failed = True
                    files_failed = -1
                except Exception as list_err: 
                    logging.error(f"✖ Error listing archive files: {list_err}", exc_info=args.debug)
                    listing_failed = True
                    files_failed = -1

                # 2. Process results (only if not in test mode)
                if not listing_failed and not args.test:
                    if files_found == 0: 
                        status_info['status'] = 'Success'
                        status_info['reason'] = 'No Files Found'
                        job_succeeded = True
                    else:
                        # Check existing, Determine needed, Download loop...
                        logging.info(f"Checking existing files...")
                        # Note: We don't need to filter PNG files here anymore since potential_files is already filtered
                        potential_filenames = []
                        for f in potential_files:
                            if not isinstance(f, dict):
                                continue
                            filename = f.get('filename')
                            if not filename:
                                continue
                            potential_filenames.append(filename)
                        
                        # Now check for existing files more precisely
                        files_to_download = []
                        for filename in potential_filenames:
                            file_path = output_path / filename
                            if file_path.exists():
                                files_existing += 1
                                files_skipped += 1
                            else:
                                files_to_download.append(filename)

                        logging.info(f"Need {len(files_to_download)} files. ({files_skipped} exist/skipped).")
                        if files_to_download:
                            logging.info("Starting archive file download...")
                            total_dl = len(files_to_download)
                            for i, filename in enumerate(files_to_download):
                                try: 
                                    dl_info = onc_client.getFile(filename=filename, overwrite=args.yes)
                                    files_downloaded += 1
                                except FileExistsError: 
                                    logging.info(f"\n Skip: {filename} (Exists)")
                                    files_skipped += 1
                                except Exception as dl_err: 
                                    logging.error(f"\n Fail DL {filename}: {dl_err}")
                                    files_failed += 1
                                finally: 
                                    percent=(i+1)/total_dl*100
                                    bar='#'*int(percent/5)+'-'*(20-int(percent/5))
                                    print(f"DL [{bar}] {i+1}/{total_dl}", end='\r')
                            print("\nDL loop finished.")
                        if files_failed == 0: 
                            job_succeeded = True
                            status_info['status'] = 'Success'
                        else: 
                            job_succeeded = False
                            status_info['status'] = 'Failed'
                            status_info['reason'] = 'Download Error(s)'
                else: 
                    job_succeeded = False
                    status_info['status'] = 'Failed'
                    status_info['reason'] = 'Listing Error'
            except Exception as arc_err: 
                logging.error(f"✖ Unexpected archive error: {arc_err}", exc_info=args.debug)
                job_succeeded = False
                status_info['status'] = 'Failed'
                status_info['reason'] = 'Unexpected Error'
            status_info['details'] = {'found': files_found, 'skipped': files_skipped, 'downloaded': files_downloaded, 'failed': files_failed}
            print(f"{'✅' if job_succeeded else '❌'} {job_status_key} - Status: {status_info['status']}" + (f" ({status_info['reason']})" if status_info['reason'] else ""))

        # === Handle HDP Audio Order (New Logic for WAV) ===
        elif job_type == 'order_hdp':
            job_status_key = f"OrderHDP_{device_code}_{ext}"
            print(f"\n--- Processing {job_status_key} ---")
            hdp_params = request_info # Parameters prepared in request_onc_jobs
            downloaded_zip_path = None
            extracted_files_count = 0
            try:
                logging.info(f"Ordering HDP data product...")
                utils.dbg("Args for onc.orderDataProduct:", hdp_params, args=args)
                # Use args.yes for overwrite flag for the downloaded zip
                order_result = onc_client.orderDataProduct(
                    filters=hdp_params,
                    includeMetadataFile=False,
                    overwrite=args.yes
                )
                utils.dbg(f"orderDataProduct result:", order_result, args=args)

                # Check result and find downloaded ZIP file
                zip_file_info = None
                if isinstance(order_result, dict) and 'downloadResults' in order_result:
                    for item in order_result.get('downloadResults', []):
                        if isinstance(item, dict) and item.get('downloaded') is True and str(item.get('file', '')).lower().endswith('.zip'):
                            zip_file_info = item
                            break # Found the downloaded zip

                if zip_file_info:
                    zip_filename = zip_file_info.get('file')
                    zip_path = output_path / zip_filename
                    downloaded_zip_path = zip_path # Store path for details
                    logging.info(f"HDP order successful. Downloaded ZIP: {zip_filename} ({utils.human_size(zip_file_info.get('size', 0))})")

                    # Unzip the file
                    logging.info(f"Extracting {zip_filename} to {output_path}...")
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            member_list = zip_ref.namelist()
                            extracted_files_count = len(member_list)
                            zip_ref.extractall(output_path)
                        logging.info(f"Successfully extracted {extracted_files_count} file(s) from {zip_filename}.")
                        job_succeeded = True
                        status_info['status'] = 'Success'
                        # Optionally delete zip file after extraction
                        # if args.delete_zip: # Example if adding a flag
                        #     try: zip_path.unlink(); logging.info(f"Deleted {zip_filename}.")
                        #     except OSError as e: logging.warning(f"Could not delete zip file {zip_filename}: {e}")
                    except zipfile.BadZipFile:
                        logging.error(f"✖ Failed to extract {zip_filename}: Invalid ZIP file.")
                        status_info['status'] = 'Failed'; status_info['reason'] = 'Bad ZIP File'
                    except FileNotFoundError:
                        logging.error(f"✖ Failed to extract {zip_filename}: ZIP file not found at {zip_path}.")
                        status_info['status'] = 'Failed'; status_info['reason'] = 'ZIP File Not Found'
                    except Exception as zip_err:
                        logging.error(f"✖ Failed to extract {zip_filename}: {zip_err}", exc_info=args.debug)
                        status_info['status'] = 'Failed'; status_info['reason'] = 'Extraction Error'
                else:
                    # orderDataProduct completed but didn't return a downloaded zip file entry
                    logging.error(f"✖ HDP order completed but no downloaded ZIP file information found in result.")
                    status_info['status'] = 'Failed'; status_info['reason'] = 'No ZIP Info in Result'

            except requests.exceptions.HTTPError as http_err: # Catch errors during orderDataProduct
                err_text = str(http_err).lower() if http_err.response else ""
                if http_err.response is not None and http_err.response.status_code == 400 and ("api error 71" in err_text or "permissions not granted" in err_text):
                    status_info['status'] = 'Skipped'; status_info['reason'] = 'Permissions Error (API 71)'; logging.warning(f"⚠ {job_status_key}: Skipped HDP order due to permissions.")
                else: status_info['status'] = 'Failed'; status_info['reason'] = 'Order Step HTTP Error'; logging.error(f"✖ {job_status_key}: HTTP error during HDP order: {http_err}")
            except Exception as order_err: # Catch other errors during orderDataProduct
                logging.error(f"✖ Unexpected error during HDP order for {device_code}: {order_err}", exc_info=args.debug)
                status_info['status'] = 'Failed'; status_info['reason'] = 'Unexpected Order Error'

            # Store HDP results details
            status_info['details'] = {'zip_file': downloaded_zip_path.name if downloaded_zip_path else None, 'extracted_count': extracted_files_count}
            print(f"{'✅' if job_succeeded else ('⚠️' if status_info['status'] == 'Skipped' else '❌')} {job_status_key} - Status: {status_info['status']}" + (f" ({status_info['reason']})" if status_info['reason'] else ""))

        # === Handle Data Product Downloads (PNG, TXT) ===
        elif job_type == 'dataproduct':
            request_id = request_info
            job_status_key = f"Req_{request_id}_{device_code}_{ext}"
            print(f"\n--- Processing {job_status_key} ---")
            trigger_fallback = False; run_info = None; run_succeeded = False
            job_status_final = 'UNKNOWN'; actual_run_id = None
            files_dp_downloaded = 0; files_dp_skipped = 0; files_dp_expected = -1
            try:
                # Step 1: Run and wait
                logging.info(f"Running request ID {request_id}...")
                try:
                    run_info = onc_client.runDataProduct(dpRequestId=request_id, waitComplete=True)
                    run_succeeded = True
                except requests.exceptions.HTTPError as http_err:
                    run_succeeded = False
                    job_status_final = 'FAILED_ON_RUN'
                    err_text = str(http_err).lower()
                    if http_err.response is not None and http_err.response.status_code == 400 and ("api error 71" in err_text or "permissions not granted" in err_text):
                        status_info['status'] = 'Skipped'
                        status_info['reason'] = 'Permissions Error (API 71)'
                        logging.warning(f"⚠ {job_status_key}: Skipped due to permissions.")
                    else:
                        status_info['status'] = 'Failed'
                        status_info['reason'] = 'Run Step HTTP Error'
                        logging.error(f"✖ {job_status_key}: HTTP error during run: {http_err}")
                except Exception as run_err:
                    run_succeeded = False
                    job_status_final = 'FAILED_ON_RUN'
                    status_info['status'] = 'Failed'
                    status_info['reason'] = 'Run Step Exception'
                    logging.error(f"✖ {job_status_key}: Unexpected error during run: {run_err}", exc_info=args.debug)

                # Step 1b: Validate run / Check status
                final_onc_status = 'UNKNOWN'
                if run_succeeded:
                    if isinstance(run_info, dict) and run_info.get('runIds'):
                        actual_run_id = run_info['runIds'][0]; files_dp_expected = run_info.get('fileCount', -1)
                        try:
                            status_check = onc_client.checkDataProduct(request_id); status_data = None
                            if isinstance(status_check, dict): status_data = status_check
                            elif isinstance(status_check, list) and status_check: status_data = status_check[0]
                            if status_data: final_onc_status = status_data.get('searchHdrStatus', 'UNKNOWN').upper()
                            else: final_onc_status = 'CHECK_FAILED'
                            job_status_final = final_onc_status; utils.dbg(f"Parsed final status: {job_status_final}", args=args)
                            if final_onc_status in ['COMPLETE', 'COMPLETED']:
                                if files_dp_expected == 0: job_succeeded = True; status_info['status'] = 'Success'; status_info['reason'] = '0 Files Generated'; logging.info("  ONC reported 0 files generated.")
                                elif files_dp_expected > 0: logging.info(f"  ONC reported {files_dp_expected} files generated.")
                                else: logging.warning("  ONC reported invalid file count.")
                            elif final_onc_status in ['FAILED', 'CANCELLED']: status_info['status'] = 'Failed'; status_info['reason'] = f'ONC Status {final_onc_status}'; run_succeeded = False; logging.error(f"✖ Request {request_id} final status: {final_onc_status}.")
                            else: status_info['status'] = 'Failed'; status_info['reason'] = f'Unknown ONC Status {final_onc_status}'; run_succeeded = False; logging.warning(f"⚠ Request {request_id} unexpected final status: {final_onc_status}.")
                        except Exception as status_err: logging.warning(f"⚠ Error checking status: {status_err}."); run_succeeded = False; job_status_final = 'STATUS_CHECK_ERROR'
                    else: logging.error("✖ Failed to get valid run info."); run_succeeded = False; job_status_final = 'INVALID_RUN_INFO'

                # Step 2: Download data product
                if run_succeeded and not job_succeeded:
                     if actual_run_id is None: logging.error("✖ Cannot download: Run ID unknown."); status_info['status'] = 'Failed'; status_info['reason'] = 'Missing Run ID'
                     else:
                        wait_before_download = 5; logging.info(f"Waiting {wait_before_download}s..."); time.sleep(wait_before_download)
                        logging.info(f"Attempting download for runId {actual_run_id}...")
                        try:
                            dl_args = dict(runId=actual_run_id, maxRetries=3, downloadResultsOnly=False, includeMetadataFile=False, overwrite=args.yes)
                            dl_result = onc_client.downloadDataProduct(**dl_args)
                            if isinstance(dl_result, list):
                                if not dl_result and files_dp_expected == 0: job_succeeded = True; status_info['status'] = 'Success'; status_info['reason'] = '0 Files Generated (Confirmed)'; files_dp_downloaded = 0; files_dp_skipped = 0
                                elif not dl_result: trigger_fallback = True; logging.warning(f"⚠ DL empty but {files_dp_expected} files expected.")
                                else: # Analyze results
                                    succ = [item for item in dl_result if isinstance(item, dict) and (str(item.get('status','')).lower() == 'complete' or item.get('downloaded') is True)]
                                    errs = [item for item in dl_result if isinstance(item, dict) and 'error' in str(item.get('status','')).lower()]
                                    skip = [item for item in dl_result if isinstance(item, dict) and str(item.get('status','')).lower() == 'skipped']
                                    files_dp_downloaded = len(succ); files_dp_skipped = len(skip)
                                    if len(errs) > 0: logging.error(f"✖ DL had {len(errs)} error(s)."); trigger_fallback = True
                                    elif files_dp_downloaded > 0 or files_dp_skipped > 0: job_succeeded = True; status_info['status'] = 'Success'; logging.info(f"✔ DL successful/skipped {files_dp_downloaded}/{files_dp_skipped} files.")
                                    else: trigger_fallback = True; logging.warning("⚠ DL status unclear.")
                            else: trigger_fallback = True; logging.warning("⚠ DL returned unexpected type.")
                        except Exception as dl_err: logging.error(f"✖ DL Error: {dl_err}", exc_info=args.debug); trigger_fallback = True

                # Step 3: Fallback for data products if needed
                if trigger_fallback and not job_succeeded:
                    if actual_run_id is None: logging.error("Cannot fallback: Run ID unknown.")
                    else:
                        # ... (Fallback logic as before) ...
                        logging.info(f"Checking status again before fallback for {request_id}...")
                        final_status_fb = 'UNKNOWN';
                        try: fb_stat=onc_client.checkDataProduct(request_id); final_status_fb=fb_stat.get('searchHdrStatus','?') if isinstance(fb_stat,dict) else (fb_stat[0].get('searchHdrStatus','?') if isinstance(fb_stat,list) and fb_stat else '?'); final_status_fb=final_status_fb.upper()
                        except Exception as e: logging.warning(f"Fallback status check failed: {e}")
                        if final_status_fb in ['COMPLETE', 'COMPLETED']:
                            logging.info(f"Proceeding with fallback for {request_id} (runId {actual_run_id}).")
                            fallback_success = _attempt_fallback_download(request_id, actual_run_id, device_code, ext, onc_client, args, run_info)
                            job_succeeded = fallback_success
                            status_info['status'] = 'Success' if fallback_success else 'Failed'
                            status_info['reason'] = 'Fallback Attempted' + (' (Succeeded/Partial)' if fallback_success else ' (Failed)')
                            files_dp_downloaded = None; files_dp_skipped = None # Fallback doesn't provide counts
                        else: logging.warning(f"Skipping fallback; status '{final_status_fb}', not COMPLETE.")

                # Final status determination
                if not job_succeeded and status_info['status'] not in ['Skipped', 'Failed']:
                    status_info['status'] = 'Failed'
                    if not status_info['reason']: status_info['reason'] = f'Processing Failed (ONC Status: {job_status_final})' if job_status_final != 'UNKNOWN' else 'Processing Failed'

            except Exception as job_err: logging.error(f"Unexpected error processing DP request {request_id}: {job_err}", exc_info=True); status_info['status'] = 'Failed'; status_info['reason'] = 'Unexpected Processing Error'

            # Store final DP status details
            status_info['details'] = {'onc_status': job_status_final, 'run_id': actual_run_id, 'files_expected': files_dp_expected, 'downloaded': files_dp_downloaded, 'skipped': files_dp_skipped}
            print(f"{'✅' if job_succeeded else ('⚠️' if status_info['status'] == 'Skipped' else '❌')} {job_status_key} - Status: {status_info['status']}" + (f" ({status_info['reason']})" if status_info['reason'] else ""))


        # === Unknown Job Type ===
        else:
             job_status_key = f"Unknown_{job_type}_{device_code}_{ext}"
             logging.error(f"Unknown job type '{job_type}' encountered.");
             status_info['status'] = 'Failed'; status_info['reason'] = 'Unknown Job Type'

        # --- Store final status and update overall success ---
        job_statuses[job_status_key] = status_info
        if status_info['status'] == 'Failed': all_jobs_overall_success = False

        # --- Common debug listing ---
        if args.debug_net:
            proc_id = request_info if job_type != 'archive' else f"Archive_{device_code}_{ext}" # Adjust ID for logging
            print(f"--- Contents of '{output_path}' after processing {proc_id} ---")
            utils.list_tree(output_path, args=args); print("-" * (58 + len(str(proc_id))))

    # --- End Job Loop ---
    return all_jobs_overall_success, job_statuses