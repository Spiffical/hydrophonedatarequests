# hydro_dl/utils.py
import logging
import os
import sys
import pathlib
import pprint
from datetime import datetime
from typing import Union, Any

try:
    from dateutil import parser as dtparse
    from dateutil.tz import gettz, UTC, tzfile
except ImportError:
    sys.exit("ERROR: 'python-dateutil' library not found. Please install it: pip install python-dateutil")

from hydrophone.config.settings import ISO_FMT

# --- Formatting & Display ---

def human_size(b: int) -> str:
    """Converts bytes to a human-readable string (KB, MB, GB, TB)."""
    if b == 0: return "0 B"
    b = float(b)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024 or u == "TB": return f"{b:,.1f} {u}"
        b /= 1024
    return f"{b:,.1f} B" # Should be unreachable, but fallback

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