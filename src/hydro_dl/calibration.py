# hydro_dl/calibration.py
from __future__ import annotations

import logging
import re # Import re for potential use, though not strictly needed now
from datetime import datetime, timezone, timedelta # Added timedelta
from typing import List, Tuple, Optional

from onc import ONC
from requests.exceptions import HTTPError

# Note: Removed direct use of utils.dbg as args object isn't easily available here.

def _select_attr_record(records: list[dict],
                        when: datetime) -> Optional[dict]:
    """
    Given the list returned by onc.getAttributes() pick the single record
    whose dateFrom ≤ *when* < dateTo (or dateTo is None).
    Selects the *latest* valid calibration if multiple overlap.
    """
    if not isinstance(when.tzinfo, timezone) and when.tzinfo is None:
         # Add timezone info if missing, assuming UTC for comparison robustness
         # Although the calling function should pass tz-aware datetime
         logging.warning("Calibration date provided without timezone, assuming UTC.")
         when = when.replace(tzinfo=timezone.utc)

    when_utc = when.astimezone(timezone.utc)
    chosen = None
    latest_start_time = datetime.min.replace(tzinfo=timezone.utc) # Track latest start

    for rec in records:
        try:
            # Ensure proper ISO parsing with timezone
            start_str = rec.get("dateFrom")
            end_str = rec.get("dateTo") # Can be None

            if not start_str: continue # Skip records without start date

            # Use fromisoformat which expects timezone info or implies local
            # Replacing Z with +00:00 makes it explicit UTC offset
            start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            # Handle end date: None means valid indefinitely (use datetime.max)
            end = datetime.max.replace(tzinfo=timezone.utc)
            if end_str:
                end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

            # Check validity range and if it's later than the current 'chosen'
            if start <= when_utc < end:
                if chosen is None or start >= latest_start_time:
                    chosen = rec
                    latest_start_time = start # Update latest start time found

        except (TypeError, ValueError, KeyError) as e:
            logging.debug(f"Skipping attribute record due to parsing error: {rec} - Error: {e}")
            continue # Skip records with parsing errors
    return chosen


def _fetch_vector_parts(onc: ONC,
                        device_code: str,
                        base_name: str,
                        when: datetime) -> Tuple[List[float], str, str]:
    """
    Download all ...PartN attributes that exist for *base_name*, concatenate
    them, and return (vector, dateFrom, dateTo from first part).
    Uses direct _doRequest call to the 'attributes' service.
    """
    part = 1
    values: List[float] = []
    date_from = ""
    date_to   = ""

    # Construct the URL for the 'attributes' service
    base_url = onc.baseUrl
    attributes_url = f"{base_url}api/attributes"

    while True:
        name = f"{base_name}Part{part}"
        filters = {
            "method": "get",
            "deviceCode": device_code,
            "attributeName": name
            # token is added automatically by _doRequest
        }
        logging.debug(f"Fetching attribute: {name} with filters: {filters}")

        try:
            # Call _doRequest directly using the discovery module's instance
            # Assumes 'discovery' is an accessible attribute of the main ONC object
            recs = onc.discovery._doRequest(attributes_url, filters)
            logging.debug(f"Result for {name}: {recs}")

            # Select the record valid for the 'when' timestamp
            chosen = _select_attr_record(recs, when)

            if not chosen or not chosen.get("value"):
                 # If first part fails, log it. Otherwise, it's normal termination.
                 if part == 1: logging.info(f"  -> Attribute {name} not found or invalid for {when.isoformat()}.")
                 else: logging.debug(f"  -> No more valid parts found for {base_name} after Part{part-1}.")
                 break # Stop if no valid record or no value

            # Capture validity range from the *first valid* part found
            if part == 1:
                date_from = chosen.get("dateFrom", "")
                date_to   = chosen.get("dateTo", "") or "" # Ensure empty string if None

            # Parse and extend values, handle potential errors
            try:
                part_values = [float(v.strip()) for v in chosen["value"].split(",") if v.strip()]
                if part_values:
                    values.extend(part_values)
                    logging.debug(f"  -> Added {len(part_values)} values from Part{part}.")
                else:
                    # Found attribute but value list was empty after split/strip
                    logging.debug(f"  -> Found {name} but value list was empty.")
                    break # Empty part usually means no more data
            except (ValueError, TypeError) as parse_err:
                 logging.warning(f"  -> Error parsing values for {name}: {parse_err}. Value was: '{chosen.get('value')}'")
                 break # Stop if values can't be parsed

            part += 1 # Increment to fetch next part

        except HTTPError as e:
            # Normal exit: Part N doesn't exist at all
            if e.response is not None and e.response.status_code == 404:
                logging.debug(f"  -> Attribute {name} not present (404). Stopping.")
                break
            # Other HTTP errors
            logging.warning(f"HTTPError {e.response.status_code if e.response else '?'} fetching {name} for {device_code}: {e}")
            break # Stop on other HTTP errors
        except Exception as exc:
            # Catch-all for unexpected errors (e.g., network, _doRequest issues)
            logging.warning(f"Could not fetch/parse {name} for {device_code}: {exc}", exc_info=True)
            break # Stop on unexpected errors

    return values, date_from, date_to


def get_hydrophone_calibration(
    onc_client: ONC,
    device_code: str,
    device_type_code: Optional[str] = None,   # kept for interface parity; unused
    date_in: Optional[datetime] = None,
    cal_type: str = ""
) -> Tuple[bool, List[float], List[float], Optional[datetime], Optional[datetime]]:
    """
    Python equivalent of MATLAB *gethydrophonecalibration*. Fetches sensitivity
    and frequency bin calibration data valid for a specific time.

    Parameters are as described in the original docstring.

    Returns tuple: (is_calibrated, sensitivities, bin_frequencies, valid_from_date, valid_to_date)
    """
    when = date_in or datetime.now(timezone.utc)
    # Ensure 'when' has timezone info for comparisons
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc) # Assume UTC if naive

    logging.info(f"Fetching calibration for {device_code} valid around {when.isoformat()} (Type: '{cal_type or 'default'}')")

    sens_name = f"Hydrophone{cal_type}SensitivityVector"
    bins_name = f"Hydrophone{cal_type}SensitivityVectorBinsLeadingEdge"

    sens, df_sens, dt_sens = _fetch_vector_parts(onc_client, device_code, sens_name, when)
    bins, df_bins, dt_bins = _fetch_vector_parts(onc_client, device_code, bins_name, when)

    # Use the validity range from the sensitivity vector as primary
    date_from = (datetime.fromisoformat(df_sens.replace("Z", "+00:00")) if df_sens else None)
    date_to   = (datetime.fromisoformat(dt_sens.replace("Z", "+00:00")) if dt_sens else None)

    # --- Validation and Logging ---
    is_cal = bool(sens) # Considered calibrated if sensitivity vector exists
    if not is_cal:
        logging.warning(f"No valid sensitivity vector found for {device_code} at {when.isoformat()}.")
        return False, [], [], None, None # Return empty/None if no sensitivity

    if bins and len(sens) != len(bins):
        logging.warning(f"Calibration bins ({len(bins)}) and sensitivities ({len(sens)}) differ "
                        f"for {device_code} – trimming to common length {min(len(bins), len(sens))}")
        n = min(len(bins), len(sens))
        sens, bins = sens[:n], bins[:n]
    elif not bins:
         logging.info(f"Sensitivity vector found ({len(sens)} points), but no frequency bins found.")
    else:
         logging.info(f"Found matching sensitivity ({len(sens)}) and frequency bins ({len(bins)}).")

    if date_from: logging.info(f" -> Calibration valid from: {date_from.isoformat()}")
    if date_to: logging.info(f" -> Calibration valid to:   {date_to.isoformat()}")
    # --- End Validation ---

    return is_cal, sens, bins, date_from, date_to