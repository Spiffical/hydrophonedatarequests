"""
Parallel processing utilities for the hydrophone data requests project.
"""

import concurrent.futures
import logging
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from datetime import datetime
from requests.exceptions import HTTPError

from hydrophone.core.onc_client import ONC
from hydrophone.utils import helpers as utils

T = TypeVar('T')

def parallel_map(
    func: Callable[[Any], T],
    items: List[Any],
    max_workers: int = 10,
    desc: str = "Processing",
    ignore_errors: bool = False,
    debug: bool = False
) -> List[T]:
    """
    Generic parallel map function that processes items in parallel and shows progress.
    
    Args:
        func: Function to apply to each item
        items: List of items to process
        max_workers: Maximum number of parallel workers
        desc: Description for progress reporting
        ignore_errors: If True, skip items that raise exceptions
        debug: If True, show more detailed error messages
    
    Returns:
        List of results in the same order as input items
    """
    results = []
    errors = []
    total = len(items)
    completed = 0
    
    def process_with_progress(item):
        nonlocal completed
        try:
            result = func(item)
            completed += 1
            # Use print for progress and sys.stdout.write to ensure \r works
            sys.stdout.write(f"\r{desc}: {completed}/{total}")
            sys.stdout.flush()
            return result
        except Exception as e:
            if debug:
                logging.error(f"Error processing item: {e}")
            if not ignore_errors:
                raise
            errors.append((item, e))
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(process_with_progress, item): item for item in items}
        results = []
        
        for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                if result is not None or not ignore_errors:
                    results.append(result)
            except Exception as e:
                if not ignore_errors:
                    executor.shutdown(wait=False)
                    raise
                errors.append((item, e))
    
    # Print newline after progress bar
    sys.stdout.write("\n")
    sys.stdout.flush()
    
    if errors and not ignore_errors:
        error_msgs = [f"{item}: {e}" for item, e in errors]
        raise Exception(f"Errors occurred during parallel processing:\n" + "\n".join(error_msgs))
    
    return results

def get_deployments_parallel(
    onc_client: ONC,
    hydrophones: List[Dict],
    max_workers: int = 10,
    debug: bool = False
) -> List[Dict]:
    """
    Fetch deployments for multiple hydrophones in parallel.
    
    Args:
        onc_client: ONC API client instance
        hydrophones: List of hydrophone device dictionaries
        max_workers: Maximum number of parallel workers
        debug: If True, show more detailed error messages
    
    Returns:
        List of all valid deployments found
    """
    def fetch_device_deployments(device: Dict) -> List[Dict]:
        device_code = device.get("deviceCode")
        if not device_code:
            return []
        
        try:
            device_deployments = onc_client.getDeployments({"deviceCode": device_code})
            if not isinstance(device_deployments, list):
                logging.warning(f"Unexpected response type for {device_code} deployments")
                return []
            
            # Add device info to each deployment
            for dep in device_deployments:
                if isinstance(dep, dict):
                    dep.update(device)
            
            return device_deployments
        except HTTPError as http_err:
            if http_err.response is not None and http_err.response.status_code == 404:
                if debug:
                    logging.debug(f"No deployments found (404) for device {device_code}")
                return []
            raise
    
    all_deployments = []
    deployments_lists = parallel_map(
        fetch_device_deployments,
        hydrophones,
        max_workers=max_workers,
        desc="Fetching deployments",
        ignore_errors=True,
        debug=debug
    )
    
    for deployments in deployments_lists:
        if deployments:
            all_deployments.extend(deployments)
    
    return all_deployments

def check_archive_files_parallel(
    onc_client: ONC,
    device_codes: List[str],
    start_utc: datetime,
    end_utc: datetime,
    max_workers: int = 10,
    debug: bool = False
) -> Dict[str, bool]:
    """
    Check multiple devices for available archive files in parallel.
    
    Args:
        onc_client: ONC API client instance
        device_codes: List of device codes to check
        start_utc: Start time in UTC
        end_utc: End time in UTC
        max_workers: Maximum number of parallel workers
        debug: If True, show more detailed error messages
    
    Returns:
        Dictionary mapping device codes to boolean indicating if files exist
    """
    def check_device_files(device_code: str) -> Tuple[str, bool]:
        archive_filters = dict(
            deviceCode=device_code,
            dateFrom=utils.iso(start_utc),
            dateTo=utils.iso(end_utc),
            returnOptions='all'
        )
        try:
            list_result = onc_client.getArchivefile(filters=archive_filters, allPages=True)
            has_files = bool(list_result.get("files", []))
            return device_code, has_files
        except Exception as e:
            if debug:
                logging.warning(f"Error checking files for device {device_code}: {e}")
            return device_code, False
    
    results = parallel_map(
        check_device_files,
        device_codes,
        max_workers=max_workers,
        desc="Checking devices",
        ignore_errors=True,
        debug=debug
    )
    
    return dict(results) 