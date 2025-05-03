# hydro_dl/utils.py
import logging
import os
import sys
import pathlib
import pprint
from datetime import datetime
from typing import Union, Any, Dict, List, Optional, Set, Tuple

try:
    from dateutil import parser as dtparse
    from dateutil.tz import gettz, UTC, tzfile
except ImportError:
    sys.exit("ERROR: 'python-dateutil' library not found. Please install it: pip install python-dateutil")

from hydrophone.config.settings import ISO_FMT
from hydrophone.core.onc_client import ONC

# --- Formatting & Display ---

def human_size(size_bytes: Union[int, float]) -> str:
    """Convert bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:3.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:3.1f} PB"

def dbg_param(msg: str, obj: Any = None, debug_on: bool = False):
    """Prints debug messages and optional object pprint if debug_on is True."""
    try:
        if debug_on:
            print(f"\n🟦 DEBUG: {msg}")
            if obj is not None:
                pprint.pprint(obj, indent=2, width=110, sort_dicts=False)
    except Exception as e:
        print(f"🟦 Error in dbg_param function: {e}")

def dbg(msg: str, obj: Any = None, args: Any = None):
    """Prints debug messages and optional object pprint if debug flags are set."""
    try:
        debug_on = False
        if args:
            try:
                debug_on = args.debug or args.debug_net
            except AttributeError:
                pass # Ignore if args object doesn't have these attributes
        dbg_param(msg, obj, debug_on)
    except Exception as e:
        print(f"🟦 Error in dbg function: {e}")

def list_tree(root: Union[str, pathlib.Path], args: Any):
    """Lists directory contents recursively if debug_net is enabled."""
    if not (args and args.debug_net):
        return
    root = pathlib.Path(root)
    print(f"📁 Listing files under {root}:")
    if not root.exists():
        print(f"   (Directory {root} does not exist)")
        return
    try:
        has_items = any(root.iterdir())
        if not has_items:
            print(f"   (Directory {root} is empty)")
            return

        file_count = 0
        dir_count = 0
        items_listed = False
        for p in sorted(root.rglob("*")):
            items_listed = True
            try:
                rel_path = p.relative_to(root)
                depth = len(rel_path.parts) - 1
                indent = "   " * (depth + 1)
                if p.is_file():
                    try:
                        size = p.stat().st_size
                        print(f"{indent}└─ {p.name} ({human_size(size)})")
                        file_count += 1
                    except Exception as stat_e:
                        print(f"{indent}└─ {p.name} (Error getting size: {stat_e})")
                elif p.is_dir() and p != root:
                    print(f"{indent}└─ {p.name}/")
                    dir_count +=1
            except Exception as e:
                print(f"   (Error processing path {p}: {e})")

        if not items_listed and has_items:
            print("   (Directory contains items not shown by rglob('*'))")
        elif file_count == 0 and dir_count == 0 and items_listed:
             # This condition might be misleading if only empty dirs exist
             pass # print("   (No files found by rglob('*'))") # Removed as potentially confusing
    except Exception as e:
        print(f"   (Could not list directory {root}: {e})")

# --- Date & Time ---

def iso(dt: datetime) -> str:
    """Converts a datetime object to an ISO 8601 UTC string."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        logging.warning(f"Naive datetime {dt}. Assuming local for UTC conversion.")
        dt = dt.astimezone() # Convert to local timezone first
    # Ensure conversion to UTC before formatting
    return dt.astimezone(UTC).strftime(ISO_FMT)

def parse_local(s: str, zone: tzfile) -> datetime:
    """Parses a string into a timezone-aware datetime object in the specified zone."""
    try:
        d = dtparse.parse(s)
        if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
            # If naive, assume it's in the target local zone
            return d.replace(tzinfo=zone)
        else:
            # If aware, convert it to the target local zone
            return d.astimezone(zone)
    except Exception as e:
        # Wrap exception for clarity
        raise dtparse.ParserError(f"Could not parse '{s}': {e}") from e

# --- Data Extraction ---

def extract_bytes_from_response(payload: dict) -> int:
    """Extracts size in bytes from various keys in ONC response."""
    if not isinstance(payload, dict): return 0

    size_keys = [
        ('downloadSize', 1), # Often present in run result, usually bytes
        ('fileSize', 1), # Bytes
        ('uncompressedFileSize', 1), # Bytes
        ('estimatedFileSize', 1), # Handle string units
        ('compressedFileSize', 1), # Bytes
        ('archiveSizeMB', 1024*1024), # MB
        ('estimatedFileSizeMB', 1024*1024), # MB
        ('expectedSizeMB', 1024*1024) # MB
    ]

    for key, factor in size_keys:
        if key in payload and payload[key] is not None:
            val = payload[key]
            try:
                if key == 'estimatedFileSize' and isinstance(val, str):
                    s = str(val).strip().upper().replace(',', '')
                    n_str = ''.join(filter(lambda x: x.isdigit() or x == '.', s))
                    if not n_str: continue
                    n = float(n_str)
                    if 'GB' in s: return int(n * 1024 * 1024 * 1024)
                    elif 'MB' in s: return int(n * 1024 * 1024)
                    elif 'KB' in s: return int(n * 1024)
                    elif 'B' in s: return int(n)
                    else: return int(n * 1024 * 1024) # Assume MB if no unit
                elif isinstance(val, (int, float)):
                    return int(float(val) * factor)
            except (ValueError, TypeError):
                continue

    return 0

def extract_mb(payload: dict, debug: bool = False) -> float:
    """
    Attempts to extract an estimated file size in Megabytes (MB) from various
    keys in an ONC API response payload. Uses extract_bytes_from_response.
    """
    bytes_val = extract_bytes_from_response(payload)
    mb = bytes_val / 1048576.0

    if debug and bytes_val > 0:
        print(f"\n╭─ Size Est. Result: {mb:,.2f} MB ({human_size(bytes_val)})")
        print(f"╰─ From Payload: {payload}")
    elif debug:
        print(f"\n🟦 DEBUG: No usable size information found in payload: {payload}")

    return mb

def retry_request(func, *args, max_retries=3, initial_wait=1, **kwargs):
    """
    Helper function to retry requests on 500 errors with exponential backoff.
    
    Args:
        func: The function to retry
        *args: Positional arguments to pass to the function
        max_retries: Maximum number of retry attempts (default: 3)
        initial_wait: Initial wait time in seconds before first retry (default: 1)
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call if successful
        
    Raises:
        The last exception encountered if all retries fail
    """
    import time
    import logging
    import requests

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 500:
                if attempt < max_retries - 1:  # Don't wait after the last attempt
                    wait_time = initial_wait * (2 ** attempt)  # Exponential backoff
                    logging.warning(f"Got 500 error, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
            raise  # Re-raise the exception if it's not a 500 error or we're out of retries

def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string to datetime object."""
    return dtparse.parse(date_str)

def ensure_dir(path: Union[str, pathlib.Path]) -> pathlib.Path:
    """Ensure directory exists, create if not."""
    path_obj = pathlib.Path(path)
    if not path_obj.exists():
        path_obj.mkdir(parents=True)
    return path_obj

def filter_deployments_with_data(
    onc_client: ONC,
    deployments: List[Dict],
    start_utc: datetime,
    end_utc: datetime,
    is_archive: bool = False,
    debug: bool = False
) -> Tuple[List[Dict], Dict[str, bool]]:
    """
    Filter deployments to only those that have data available in the specified time range.
    
    Args:
        onc_client: ONC API client instance
        deployments: List of deployment dictionaries
        start_utc: Start time in UTC
        end_utc: End time in UTC
        is_archive: Whether to check for archive files (True) or data products (False)
        debug: Whether to show debug logging
    
    Returns:
        Tuple of (filtered deployments list, dict mapping device codes to data availability)
    """
    from hydrophone.utils.parallel import check_archive_files_parallel
    
    # Get unique device codes
    device_codes = set()
    device_to_deployments = {}
    for dep in deployments:
        device_code = dep.get('deviceCode')
        if device_code:
            device_codes.add(device_code)
            if device_code not in device_to_deployments:
                device_to_deployments[device_code] = []
            device_to_deployments[device_code].append(dep)
    
    if not device_codes:
        return [], {}
        
    # Check for data availability
    if is_archive:
        # Use parallel check for archive files
        device_has_data = check_archive_files_parallel(
            onc_client,
            list(device_codes),
            start_utc,
            end_utc,
            max_workers=10,
            debug=debug
        )
    else:
        # Check for data products
        device_has_data = {}
        for device_code in device_codes:
            try:
                prod_opts = onc_client.getDataProducts({"deviceCode": device_code})
                has_products = bool(prod_opts and isinstance(prod_opts, list) and prod_opts)
                device_has_data[device_code] = has_products
                if debug and not has_products:
                    logging.debug(f"No data products found for device {device_code}")
            except Exception as e:
                if debug:
                    logging.warning(f"Error checking data products for device {device_code}: {e}")
                device_has_data[device_code] = False
    
    # Filter deployments to only those with data
    filtered_deployments = []
    for device_code, has_data in device_has_data.items():
        if has_data and device_code in device_to_deployments:
            filtered_deployments.extend(device_to_deployments[device_code])
    
    if debug:
        total_devices = len(device_codes)
        devices_with_data = sum(1 for has_data in device_has_data.values() if has_data)
        logging.debug(f"Found {devices_with_data}/{total_devices} devices with available data")
    
    return filtered_deployments, device_has_data