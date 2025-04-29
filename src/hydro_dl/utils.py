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

from .config import ISO_FMT

# --- Formatting & Display ---

def human_size(b: int) -> str:
    """Converts bytes to a human-readable string (KB, MB, GB, TB)."""
    if b == 0: return "0 B"
    b = float(b)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024 or u == "TB": return f"{b:,.1f} {u}"
        b /= 1024
    return f"{b:,.1f} B" # Should be unreachable, but fallback

def dbg(msg: str, obj: Any = None, args: Any = None):
    """Prints debug messages and optional object pprint if debug flags are set."""
    try:
        if args and (args.debug or args.debug_net):
            print(f"\n🟦 DEBUG: {msg}")
            if obj is not None:
                pprint.pprint(obj, indent=2, width=110, sort_dicts=False)
    except AttributeError: # Handle case where args might not have debug flags
        pass
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

def extract_mb(payload: dict, debug: bool = False) -> float:
    """
    Attempts to extract an estimated file size in Megabytes (MB) from various
    keys in an ONC API response payload.
    """
    choice, mb = "<none>", 0.0
    if not isinstance(payload, dict):
        if debug: print(f"\n🟦 DEBUG: Invalid payload type for size extraction: {type(payload)}")
        return 0.0

    # Map known size keys to their unit factor relative to Bytes
    # (e.g., MB keys need * 1048576 to get Bytes)
    val_map = {
        "fileSize": 1.0,                        # Already in Bytes
        "compressedFileSize": 1.0,              # Already in Bytes
        "archiveSizeMB": 1048576.0,             # In MB
        "estimatedFileSize": 1.0,               # Special handling for string values like "1.2 GB"
        "estimatedFileSizeMB": 1048576.0,       # In MB
        "expectedSizeMB": 1048576.0             # In MB
    }
    bytes_val = 0.0

    for key, factor_to_bytes in val_map.items():
        if key in payload and payload[key] is not None:
            val = payload[key]
            try:
                if key == "estimatedFileSize" and isinstance(val, str):
                    # Handle strings like "1.2 GB", "500 MB", "1024 KB", "512 B"
                    s = str(val).strip().upper().replace(',', '')
                    n_str = ''.join(filter(lambda x: x.isdigit() or x == '.', s))
                    if not n_str: continue # Skip if no number found
                    n = float(n_str)
                    if 'GB' in s: bytes_val = n * 1024 * 1048576; choice = key; break
                    elif 'MB' in s: bytes_val = n * 1048576; choice = key; break
                    elif 'KB' in s: bytes_val = n * 1024; choice = key; break
                    elif 'B' in s: bytes_val = n; choice = key; break
                    else: bytes_val = n * 1048576; choice = key; break # Assume MB if no unit
                elif isinstance(val, (int, float)):
                    # Handle numeric values, converting using the factor
                    bytes_val = float(val) * factor_to_bytes
                    choice = key
                    break # Found a valid numeric size, stop searching
                else:
                     if debug: print(f"🟦 DEBUG: Skipping key '{key}' due to unexpected type: {type(val)}")

            except (ValueError, TypeError) as parse_err:
                if debug: print(f"🟦 DEBUG: Could not parse size for key '{key}' with value '{val}': {parse_err}")
                continue # Try next key

    if bytes_val > 0:
        mb = bytes_val / 1048576.0

    if debug and choice != "<none>":
        print(f"\n╭─ Size Est. Key: '{choice}' (Value: {payload.get(choice, 'N/A')})")
        # print(f"│  Payload: {payload}") # Optionally print full payload
        print(f"╰─ Calculated: {mb:,.2f} MB ({human_size(int(bytes_val))})")
    elif debug:
         print(f"\n🟦 DEBUG: No usable size information found in payload: {payload}")

    return mb