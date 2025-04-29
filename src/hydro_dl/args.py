# hydro_dl/args.py
import argparse
import textwrap

def setup_arg_parser():
    """Sets up and parses command-line arguments."""
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Downloads hydrophone data (WAV, PNG, TXT) from Ocean Networks Canada.""")
    )
    ap.add_argument("--token", help="ONC API token (or set $ONC_TOKEN)")
    ap.add_argument("--start", help="Start date/time (local tz, e.g., 'YYYY-MM-DD HH:MM')")
    ap.add_argument("--end", help="End date/time (local tz, e.g., 'YYYY-MM-DD HH:MM')")
    ap.add_argument("--tz", default="America/Vancouver", help="Local timezone (default: America/Vancouver)")
    ap.add_argument("--wav", action="store_true", help="Download WAV files")
    ap.add_argument("--png", action="store_true", help="Download spectrogram PNG files")
    ap.add_argument("--txt", action="store_true", help="Download metadata TXT files")
    ap.add_argument("-o", "--output", default="downloads", help="Output directory (default: downloads)")
    ap.add_argument("-y", "--yes", action="store_true", help="Assume 'yes' to confirmation")
    ap.add_argument("--debug", action="store_true", help="Print debug info")
    ap.add_argument("--debug-net", action="store_true", help="Print network debug info (implies --debug)")
    ap.add_argument("--fallback-retries", type=int, default=12, help="Max fallback retries (default: 12)")
    ap.add_argument("--fallback-wait", type=float, default=5.0, help="Fallback wait time (s) (default: 5.0)")
    return ap.parse_args()