# hydro_dl/args.py
import argparse
import textwrap

def setup_arg_parser():
    """Sets up and parses command-line arguments."""
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Downloads hydrophone data from Ocean Networks Canada.
            
            Two modes of operation:
            1. Archive Mode (--archive): Lists and downloads raw archived files
            2. Data Product Mode (default): Generates and downloads processed data products""")
    )
    ap.add_argument("--token", help="ONC API token (or set $ONC_TOKEN)")
    ap.add_argument("--start", help="Start date/time (local tz, e.g., 'YYYY-MM-DD HH:MM')")
    ap.add_argument("--end", help="End date/time (local tz, e.g., 'YYYY-MM-DD HH:MM')")
    ap.add_argument("--tz", default="America/Vancouver", help="Local timezone (default: America/Vancouver)")
    
    # Mode selection
    ap.add_argument("--archive", action="store_true", help="Use archive mode to list and download raw archived files")
    ap.add_argument("--test", action="store_true", help="Test mode: only list available files without downloading")
    
    # Data product specific options (only used in data product mode)
    product_group = ap.add_argument_group('Data Product Options (only used when not in archive mode)')
    product_group.add_argument("--flac", action="store_true", help="Download FLAC audio files")
    product_group.add_argument("--png", action="store_true", help="Download spectrogram PNG files")
    product_group.add_argument("--txt", action="store_true", help="Download metadata TXT files")
    
    # Common options
    ap.add_argument("-o", "--output", default="downloads", help="Output directory (default: downloads)")
    ap.add_argument("-y", "--yes", action="store_true", help="Assume 'yes' to confirmation")
    ap.add_argument("--debug", action="store_true", help="Print debug info")
    ap.add_argument("--debug-net", action="store_true", help="Print network debug info (implies --debug)")
    ap.add_argument("--fallback-retries", type=int, default=12, help="Max fallback retries (default: 12)")
    ap.add_argument("--fallback-wait", type=float, default=5.0, help="Fallback wait time (s) (default: 5.0)")
    ap.add_argument("--fetch-sensitivity", action="store_true", help="Fetch and save hydrophone sensitivity calibration file(s)")
    return ap.parse_args()