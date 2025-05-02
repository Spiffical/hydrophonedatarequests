"""
Interactive UI module for Google Colab integration with the hydrophone data requests project.
"""

import ipywidgets as widgets
from IPython.display import display, clear_output, HTML
from datetime import datetime, time
import logging
import sys
import os
import re
import pytz  # For comprehensive timezone list
from collections import defaultdict
from dateutil.tz import gettz, UTC
import traceback  # Add import for better error logging

# Ensure src directory is in path for imports
src_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import project modules
from hydrophone.core import onc_client as onc
from hydrophone.core import downloader as core_downloader
from hydrophone.utils import helpers as utils
from hydrophone.utils.exceptions import (
    ConfigError, UserAbortError, NoDataError,
    ONCInteractionError, HydroDLError, DownloadError
)
from hydrophone.config import settings
from hydrophone.core.onc_client import ONC

# Import files module for download helpers if needed later
try:
    from google.colab import drive, files
    COLAB_ENV = True
except ImportError:
    COLAB_ENV = False

# Import FileChooser and its utilities
try:
    from ipyfilechooser import FileChooser
    from ipyfilechooser.utils import InvalidPathError
    FILECHOOSER_AVAILABLE = True
except ImportError:
    FileChooser = None
    InvalidPathError = None
    FILECHOOSER_AVAILABLE = False
    print("WARNING: 'ipyfilechooser' not found. Drive folder picker will not be available. Install with: pip install ipyfilechooser", file=sys.stderr)

# --- Constants ---
DEFAULT_TIMEZONES = [
    'UTC', 'America/Vancouver', 'America/Toronto', 'America/New_York',
    'Europe/London', 'Europe/Berlin', 'Asia/Tokyo', 'Australia/Sydney'
] + sorted([tz for tz in pytz.common_timezones if '/' in tz and tz not in [
    'UTC', 'America/Vancouver', 'America/Toronto', 'America/New_York', 
    'Europe/London', 'Europe/Berlin', 'Asia/Tokyo', 'Australia/Sydney'
]])

# Google Drive Mount Path
DRIVE_MOUNT_PATH = '/content/drive'
DRIVE_MYDRIVE_PATH = os.path.join(DRIVE_MOUNT_PATH, 'MyDrive')

# --- Add CSS for spinner animation ---
display(HTML("""
<style>
/* Give every <i class="fa fa-spinner"> inside a .loading class a spin */
.loading i.fa-spinner {
  animation: fa-spin 1s infinite linear;
}
@keyframes fa-spin {           /* fallback if fa-spin is missing */
  0%   { transform: rotate(0); }
  100% { transform: rotate(360deg); }
}
</style>
"""))

# --- Widget Definitions ---

# Setup Widgets
w_token = widgets.Password(description="ONC Token:", layout=widgets.Layout(width='95%'))

# Use HBox for date/time alignment
w_start_hbox = widgets.HBox([widgets.DatePicker(description='Start Date:', layout=widgets.Layout(width='auto')), widgets.Label("Time (HH:MM):"), widgets.Text(description='', value='00:00', placeholder='HH:MM', layout=widgets.Layout(width='100px'))])
w_end_hbox = widgets.HBox([widgets.DatePicker(description='End Date:', layout=widgets.Layout(width='auto')), widgets.Label("Time (HH:MM):"), widgets.Text(description='', value='00:00', placeholder='HH:MM', layout=widgets.Layout(width='100px'))])

w_tz = widgets.Dropdown(
    options=DEFAULT_TIMEZONES,
    value='America/Vancouver',
    description="Timezone:",
    style={'description_width': 'initial'},
    layout=widgets.Layout(min_width='300px', width='auto')
)
w_output_dir = widgets.Text(
    description="Output Dir:",
    value="downloads",
    layout=widgets.Layout(width='auto', min_width='300px')
)

# --- Download Location Widgets ---
w_download_target = widgets.RadioButtons(
    options=['Colab Environment', 'Google Drive'],
    value='Colab Environment',
    description='Download To:',
    disabled=False,
    layout={'width': 'max-content'}
)
w_colab_path = widgets.Text(description="Colab Path:", value="downloads", layout=widgets.Layout(width='auto', min_width='300px'))

# Container for Drive-related widgets
w_download_drive_container = widgets.VBox([], layout=widgets.Layout(width='95%'))

w_drive_mount_instruct = widgets.HTML(value="""
<div style="margin-top: 5px; font-size: small; color: grey;">
    <i>Ensure Google Drive is mounted. Run this in a cell:</i><br>
    <pre style="background:#f0f0f0; padding: 3px;">from google.colab import drive\ndrive.mount('/content/drive', force_remount=True)</pre>
    <i>Then select the target folder above.</i>
</div>
""", layout=widgets.Layout(display='none'))

# Group download location widgets
w_download_location_box = widgets.VBox([
    w_download_target,
    w_colab_path,
    w_download_drive_container,
    w_drive_mount_instruct
])
# --- End Download Location Widgets ---

# Mode & Options Widgets
w_mode = widgets.RadioButtons(
    options=['Request New Data Product', 'Request Archived Data'],
    description='Mode:',
    value='Request New Data Product',
    layout={'width': 'max-content'}
)
w_fetch_sensitivity = widgets.Checkbox(value=False, description='Fetch Sensitivity')
w_debug = widgets.Checkbox(value=False, description='Debug Logs')
w_debug_net = widgets.Checkbox(value=False, description='Network Logs')
w_opts_hbox = widgets.HBox([
    w_fetch_sensitivity,
    w_debug,
    w_debug_net
], layout=widgets.Layout(margin='5px 0 0 0'))

# Action Buttons
w_discover_btn = widgets.Button(
    description="1. Discover Locations & Devices",
    button_style='info',
    icon='search',
    layout=widgets.Layout(width='95%')
)
w_download_btn = widgets.Button(
    description="3. Start Download / Process",
    button_style='success',
    icon='download',
    disabled=True,
    layout=widgets.Layout(width='95%')
)

# Dynamic Selection Widgets
w_location_label = widgets.HTML("<b>2a. Select Location:</b>")
w_location_select = widgets.Select(
    description="",
    options=[],
    rows=6,
    layout=widgets.Layout(width='95%', min_height='100px'),
    disabled=True
)

w_device_label = widgets.HTML("<b>2b. Select Device(s):</b>")
w_device_select = widgets.SelectMultiple(
    description="",
    options=[],
    rows=8,
    layout=widgets.Layout(width='95%', min_height='120px'),
    disabled=True
)

w_product_archive_label = widgets.HTML("")
w_product_selection_area = widgets.VBox([], layout=widgets.Layout(width='95%'))
w_archive_selection_area = widgets.VBox([], layout=widgets.Layout(width='95%'))

# Status Label
w_discover_status_label = widgets.HTML(
    value="",
    layout=widgets.Layout(margin='5px 0 5px 0')
)
w_download_status_label = widgets.HTML(
    value="",
    layout=widgets.Layout(margin='5px 0 5px 0')
)

# Output Widget
w_output_area = widgets.Output(
    layout={
        'border': '1px solid black',
        'height': '400px',
        'overflow_y': 'scroll',
        'width': '95%'
    }
)

# --- State Variables ---
state = {
    "onc_service": None,  # Store initialized client
    "deployments": [],
    "location_map": {},
    "parent_loc_choices": [],
    "parent_loc_codes": [],
    "parent_choice_details": {},
    "devices_at_selected_location": {},
    "selected_parent_code": None,
    "available_products": {},  # {ext: [product_dict]} - Used for Data Product Mode
    "available_archive_files": [],  # List of file info dicts - Used for Archive Mode
    "product_checkboxes": {},  # {(prod_code, ext): checkbox_widget}
    "archive_checkboxes": {},  # {ext: checkbox_widget}
    "chosen_deployments": [],
    "all_params": {},
    "drive_file_chooser_instance": None  # Store the instance if created
}

# --- Helper Functions ---
def _get_datetime_from_widgets(date_widget, time_widget, tz_widget):
    """Combines date and time widgets into a timezone-aware datetime."""
    date_val = date_widget.value
    time_str = time_widget.value
    tz_str = tz_widget.value

    if not date_val:
        show_status("✖ Please select a date.", target='discover', error=True)
        return None
    if not tz_str:
        show_status("✖ Please select a timezone.", target='discover', error=True)
        return None

    local_zone = gettz(tz_str)
    if local_zone is None:
        show_status(f"✖ Invalid timezone: {tz_str}", target='discover', error=True)
        return None

    try:
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Time out of range")
        dt_naive = datetime.combine(date_val, time(hour, minute))
        dt_aware = dt_naive.replace(tzinfo=local_zone)
        return dt_aware
    except Exception as e:
        show_status(f"✖ Invalid time format '{time_str}'. Use HH:MM. Error: {e}", target='discover', error=True)
        return None

def _set_button_state(btn, *, working):
    """Flip a single button between idle and 'working'."""
    if working:
        btn.add_class('loading')
        btn.icon = 'spinner'                 # plain FA name
        btn.disabled = True                  # grey-out *but* keep label
    else:
        btn.remove_class('loading')
        # restore each button's own idle icon
        btn.icon = 'search' if btn is w_discover_btn else 'download'
        btn.disabled = False

def show_status(message, *, target='status', working=False, error=False):
    """
    Update status bar **and** (optionally) one button.
        target : 'discover' | 'download' | 'status'  (default)
        working: True while the action is running.
    """
    colour = 'red' if error else ('blue' if working else 'green')
    icon_css = 'times' if error else ('spinner fa-spin' if working else 'check')
    status_html = f'<div style="color:{colour}"><i class="fa fa-{icon_css}"></i> {message}</div>'
    
    if target == 'discover':
        w_discover_status_label.value = status_html
    elif target == 'download':
        w_download_status_label.value = status_html
    else: # Fallback or general status
        w_discover_status_label.value = status_html # Default to discover status for now
        w_download_status_label.value = "" # Clear download status

    if target == 'discover':
        _set_button_state(w_discover_btn, working=working)
    elif target == 'download':
        _set_button_state(w_download_btn, working=working)

def clear_status():
    """Clears the status message and resets button icons."""
    w_discover_status_label.value = ""
    w_download_status_label.value = ""
    _set_button_state(w_discover_btn, working=False)
    _set_button_state(w_download_btn, working=False)

def _toggle_widgets(disabled: bool):
    """Enable/disable widgets during processing, refining download button logic."""
    # Determine if selections are complete enough to potentially enable download
    selections_ready = False
    if state.get("chosen_deployments"):  # Must have deployments selected
        is_archive = (w_mode.value == 'Request Archived Data')
        if is_archive:
            # Archive mode: Ready if archive checkboxes dict *exists* (UI was built)
            selections_ready = state.get("archive_checkboxes") is not None  # Check existence, not emptiness
        else:
            # Data Product mode: Ready if product checkboxes dict *exists* (UI was built)
            selections_ready = state.get("product_checkboxes") is not None  # Check existence, not emptiness

    # --- Disable/Enable Widgets ---
    w_token.disabled = disabled
    w_start_hbox.children[0].disabled = disabled
    w_start_hbox.children[2].disabled = disabled
    w_end_hbox.children[0].disabled = disabled
    w_end_hbox.children[2].disabled = disabled
    w_tz.disabled = disabled
    w_output_dir.disabled = disabled
    w_mode.disabled = disabled
    w_fetch_sensitivity.disabled = disabled
    w_debug.disabled = disabled
    w_debug_net.disabled = disabled

    # Buttons
    w_discover_btn.disabled = disabled
    # Download button: Disabled if processing OR if selections are not yet ready
    w_download_btn.disabled = disabled or not selections_ready

    # Selection widgets
    # Location dropdown only enabled if choices exist AND not processing
    w_location_select.disabled = disabled or not state.get("parent_loc_choices")
    # Device multiselect only enabled if location selected AND not processing
    w_device_select.disabled = disabled or not state.get("selected_parent_code")

    # Dynamic areas: Disable all child interactives if processing
    for area in [w_product_selection_area, w_archive_selection_area]:
        for w in area.children:
            # Handle Accordion children specifically
            if isinstance(w, widgets.Accordion):
                for child_vbox in w.children:
                    for checkbox in child_vbox.children:
                        checkbox.disabled = disabled
            elif hasattr(w, 'disabled'):  # Handle Checkboxes, Buttons etc.
                w.disabled = disabled

    # Also disable FileChooser when processing
    fc_instance = state.get("drive_file_chooser_instance")
    if FILECHOOSER_AVAILABLE and isinstance(fc_instance, FileChooser):
        fc_instance.disabled = disabled

def _build_product_selection_ui(available_products):
    """Dynamically creates product selection widgets (Checkboxes in Accordion)."""
    state["product_checkboxes"] = {}  # Clear previous
    accordion_children = []
    sorted_exts = sorted(available_products.keys())

    if not sorted_exts:
        w_product_archive_label.value = "<i>No specific data products found for this device/time.</i>"
        return []

    for ext in sorted_exts:
        products = available_products[ext]
        ext_checkboxes = []
        for p_dict in products:
            prod_code = p_dict.get('dataProductCode', 'N/A')
            prod_name = p_dict.get('dataProductName', 'Unknown')
            cb = widgets.Checkbox(
                description=f"{prod_name} ({prod_code})",
                value=False,
                indent=True,
                layout=widgets.Layout(width='auto')  # Allow natural width
            )
            state["product_checkboxes"][(prod_code, ext)] = cb
            ext_checkboxes.append(cb)
        ext_vbox = widgets.VBox(ext_checkboxes)
        accordion_children.append(ext_vbox)

    accordion = widgets.Accordion(
        children=accordion_children,
        layout=widgets.Layout(width='95%')  # Accordion width
    )
    for i, ext in enumerate(sorted_exts):
        accordion.set_title(i, f"{ext.upper()} Products ({len(available_products[ext])})")

    w_product_archive_label.value = "<b>2c. Select Data Products:</b>"
    return [accordion]

def _build_archive_selection_ui(archive_files_info):
    """Dynamically creates archive file type selection widgets."""
    state["archive_checkboxes"] = {}  # Clear previous
    files_by_ext = defaultdict(lambda: {'count': 0, 'size': 0, 'filenames': []})
    total_files = 0
    total_size = 0

    for file_info in archive_files_info:
        if not isinstance(file_info, dict): continue
        filename = file_info.get('filename')
        if not filename: continue

        # Filter out thumbs/small PNGs
        is_png = '.png' in filename.lower()
        is_thumb_small = is_png and ('-small.png' in filename.lower() or '-thumb.png' in filename.lower())
        if is_thumb_small: continue

        total_files += 1
        file_size = file_info.get('uncompressedFileSize', file_info.get('fileSize', 0))
        total_size += file_size
        ext = filename.split('.')[-1].lower() if '.' in filename else 'unknown'

        files_by_ext[ext]['count'] += 1
        files_by_ext[ext]['size'] += file_size
        if len(files_by_ext[ext]['filenames']) < 3:
            files_by_ext[ext]['filenames'].append(filename)

    if not files_by_ext:
        w_product_archive_label.value = "<i>No downloadable archive files found for this device/time.</i>"
        return []

    w_product_archive_label.value = f"<b>2c. Select Archive File Types ({total_files} files, {utils.human_size(total_size)} total):</b>"
    
    archive_widgets = []
    sorted_exts = sorted(files_by_ext.keys())

    for ext in sorted_exts:
        info = files_by_ext[ext]
        cb = widgets.Checkbox(
            description=f"{ext.upper()} Files ({info['count']}, {utils.human_size(info['size'])})",
            value=False,
            indent=True,
            layout=widgets.Layout(width='auto')  # Allow natural width
        )
        state["archive_checkboxes"][ext] = cb
        archive_widgets.append(cb)

    return archive_widgets

def _prepare_parent_location_choices(deployments, loc_map):
    """Helper to group deployments and format choices for the location selector."""
    by_parent_loc = defaultdict(list)
    parent_codes_found = set()
    for d in deployments:
        loc_code_from_dep = d.get('locationCode')
        if not loc_code_from_dep:
            continue
        parent_code = loc_code_from_dep.split('.')[0]
        by_parent_loc[parent_code].append(d)
        parent_codes_found.add(parent_code)

    if not by_parent_loc:
        raise NoDataError("No processable locations found in deployments.")

    sorted_parent_codes = sorted(list(parent_codes_found))
    parent_loc_choices = []
    parent_choice_details = {}
    GENERIC_LOC_MAP_NAMES = {"Hydrophone Array - Box Type", "Underwater Network"}

    for parent_code in sorted_parent_codes:
        display_name = None
        deployments_at_parent = by_parent_loc[parent_code]
        first_deployment = deployments_at_parent[0] if deployments_at_parent else None

        # Naming Logic
        parent_map_name = loc_map.get(parent_code)
        is_generic_from_map = bool(parent_map_name and parent_map_name in GENERIC_LOC_MAP_NAMES)

        if parent_map_name and not is_generic_from_map:
            display_name = parent_map_name
        elif first_deployment:
            citation_text = first_deployment.get('citation', {}).get('citation')
            citation_name = onc._extract_name_from_citation(citation_text)
            if citation_name:
                display_name = citation_name

        if display_name is None:
            display_name = parent_map_name if is_generic_from_map else parent_code

        # Get Device List
        device_codes_only = set(dep.get('deviceCode') for dep in deployments_at_parent if dep.get('deviceCode'))
        device_list_str = f" (Devs: {len(device_codes_only)})" if device_codes_only else ""

        parent_choice_details[parent_code] = {
            'display_name': display_name,
            'all_deployments': deployments_at_parent,
            'device_codes': sorted(list(device_codes_only))
        }
        final_choice_string = f"{display_name} [{parent_code}]{device_list_str}"
        parent_loc_choices.append(final_choice_string)

    return parent_loc_choices, sorted_parent_codes, parent_choice_details

# --- Widget Callbacks ---
def on_discover_button_clicked(b):
    """Handles the 'Discover Locations & Devices' button click."""
    _toggle_widgets(True)
    _set_button_state(w_discover_btn, working=True)
    show_status("Discovering...", target='discover', working=True)
    # Clear previous selections etc.
    w_location_select.options = []; w_device_select.options = []; w_product_selection_area.children = []
    w_archive_selection_area.children = []; w_product_archive_label.value = ""; w_download_btn.disabled = True
    state.clear(); w_output_area.clear_output(wait=True)

    try:
        with w_output_area:
            print("--- Discovering Deployments ---")
            # 1. Collect and Validate Inputs
            token = w_token.value
            # --- Get download path based on selection ---
            target_type = w_download_target.value
            output_dir = None  # Initialize
            if target_type == 'Google Drive':
                # Check if FileChooser instance exists and has a path selected
                fc_instance = state.get("drive_file_chooser_instance")
                if FILECHOOSER_AVAILABLE and isinstance(fc_instance, FileChooser):
                    selected_drive_path = fc_instance.selected_path
                    if not selected_drive_path:
                        show_status("✖ Google Drive selected, but no folder chosen in the picker.", target='discover', error=True)
                        _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
                    # Basic check if drive seems mounted (using the root)
                    if COLAB_ENV and not os.path.isdir(DRIVE_MOUNT_PATH):
                        show_status("✖ Google Drive selected, but '/content/drive' not found. Please mount Drive.", target='discover', error=True)
                        _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
                    # Use the path selected by the user
                    output_dir = selected_drive_path
                else:  # FileChooser not available or failed
                    # Try to get path from manual input if present
                    if w_download_drive_container.children and isinstance(w_download_drive_container.children[-1], widgets.VBox):
                        manual_input = w_download_drive_container.children[-1].children[-1]
                        if isinstance(manual_input, widgets.Text) and manual_input.value.strip():
                            output_dir = manual_input.value.strip()
                        else:
                            show_status("✖ Please enter a valid Google Drive path.", target='discover', error=True)
                            _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
                    else:
                        show_status("✖ No valid Google Drive path input method available.", target='discover', error=True)
                        _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
            else:  # Colab Environment
                # Get the parent directory of the current workspace
                parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                # Use the specified path or default to 'downloads', but place it at parent level
                colab_path = w_colab_path.value.strip() or 'downloads'
                # If path is relative, make it relative to parent directory
                if not os.path.isabs(colab_path):
                    output_dir = os.path.join(os.path.dirname(parent_dir), colab_path)
                else:
                    output_dir = colab_path

            if not output_dir:  # Should not happen if logic above is correct
                show_status("✖ Could not determine output directory.", target='discover', error=True)
                _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
            # --- End get download path ---

            tz_str = w_tz.value
            is_archive = (w_mode.value == 'Request Archived Data'); is_test = is_archive
            start_dt = _get_datetime_from_widgets(w_start_hbox.children[0], w_start_hbox.children[2], w_tz) # Get from HBox children
            end_dt = _get_datetime_from_widgets(w_end_hbox.children[0], w_end_hbox.children[2], w_tz) # Get from HBox children

            if not token: show_status("✖ ONC Token is required.", target='discover', error=True); _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
            if not start_dt or not end_dt: _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return
            if end_dt <= start_dt: show_status("✖ End date/time must be after start.", target='discover', error=True); _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return

            state['all_params'] = {'token': token, 'start_dt': start_dt, 'end_dt': end_dt, 'tz': tz_str, 'output': output_dir, 'archive': is_archive, 'test': is_test, 'fetch_sensitivity': w_fetch_sensitivity.value, 'debug': w_debug.value or w_debug_net.value, 'debug_net': w_debug_net.value, 'yes': True, 'fallback_retries': 12, 'fallback_wait': 5.0,}
            log_level = logging.DEBUG if state['all_params']['debug'] else logging.INFO; logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s', stream=sys.stdout, force=True)

            # 2. Initialize ONC Client
            print(f"Output target: {output_dir}") # Show final path
            print("Connecting to ONC..."); show_status("Connecting to ONC...", target='discover', working=True)
            onc_service = ONC(token, outPath=output_dir, showInfo=state['all_params']['debug_net'], timeout=settings.DEFAULT_ONC_TIMEOUT)
            onc_service.getLocations({}); state["onc_service"] = onc_service; print("Connection successful.")

            # 3. Find Deployments
            print("Finding overlapping deployments..."); show_status("Finding deployments...", target='discover', working=True)
            fake_args = type('obj', (object,), state['all_params'])()
            start_utc = start_dt.astimezone(UTC); end_utc = end_dt.astimezone(UTC)
            deployments, loc_map = onc.find_overlapping_deployments(onc_service, start_utc, end_utc, fake_args)
            state['deployments'] = deployments; state['location_map'] = loc_map
            print(f"Found {len(deployments)} potentially relevant deployment(s).")
            if not deployments: show_status("No overlapping deployments found.", target='discover', error=False); _toggle_widgets(False); _set_button_state(w_discover_btn, working=False); return

            # 4. Populate Location Selector
            parent_choices, parent_codes, parent_details = _prepare_parent_location_choices(deployments, loc_map)
            state["parent_loc_choices"] = parent_choices; state["parent_loc_codes"] = parent_codes
            state["parent_choice_details"] = parent_details; w_location_select.options = parent_choices
            w_location_select.disabled = False; print("\n== Please select a Parent Location below ==")
            show_status("Discovery complete. Select Location.", target='discover', working=False)

    except NoDataError as e: show_status(f"ℹ️ {e}", target='discover', error=False)
    except Exception as e: show_status(f"✖ Discovery Error: {e}", target='discover', error=True); logging.exception("Discovery error:")
    finally:
        _toggle_widgets(False) # Re-enable widgets
        _set_button_state(w_discover_btn, working=False) # Ensure button state is idle
        w_download_btn.disabled = True # Keep download disabled

def on_location_selected(change):
    """Handles selection changes in the location dropdown."""
    _toggle_widgets(True)
    show_status("Processing location selection...", target='discover', working=True)
    # Clear downstream widgets
    w_device_select.options = []
    w_device_select.value = []
    w_device_select.disabled = True
    w_product_selection_area.children = []
    w_archive_selection_area.children = []
    w_download_btn.disabled = True
    state["selected_parent_code"] = None
    state["devices_at_selected_location"] = {}
    state["chosen_deployments"] = []
    state["available_products"] = {}
    state["available_archive_files"] = []
    state["product_checkboxes"] = {}
    state["archive_checkboxes"] = {}

    if not change['new']:
        show_status("Location unselected.", target='discover', working=False)
        _toggle_widgets(False)
        return

    selected_display_string = change['new']
    try:
        selected_index = state["parent_loc_choices"].index(selected_display_string)
        selected_code = state["parent_loc_codes"][selected_index]
        state["selected_parent_code"] = selected_code
    except (ValueError, IndexError):
        show_status(f"✖ Error identifying selected location: {selected_display_string}", target='discover', error=True)
        _toggle_widgets(False)
        return

    details = state["parent_choice_details"].get(selected_code)
    if not details:
        show_status(f"✖ Internal error: No details for location {selected_code}", target='discover', error=True)
        _toggle_widgets(False)
        return

    # Populate device selector
    state["devices_at_selected_location"].clear()
    device_options = ["ALL Hydrophones at this location"]
    sorted_device_codes = details['device_codes']

    # Get device names
    temp_device_names = {}
    for dep in details['all_deployments']:
        d_code = dep.get('deviceCode')
        d_name = dep.get('deviceName', 'Unknown Device')
        if d_code and d_code not in temp_device_names:
            temp_device_names[d_code] = d_name
    state["devices_at_selected_location"] = temp_device_names

    device_options.extend([f"{temp_device_names.get(code, 'Unknown')} ({code})" for code in sorted_device_codes])

    w_device_select.options = device_options
    w_device_select.disabled = False
    show_status("Location selected. Select Device(s).", target='discover', working=False)
    _toggle_widgets(False)

def on_device_selected(change):
    """Handles selection changes in the device multi-select."""
    _toggle_widgets(True)
    show_status("Processing device selection...", target='discover', working=True)
    # Clear dynamic UI areas
    w_product_selection_area.children = []
    w_archive_selection_area.children = []
    w_download_btn.disabled = True
    state["chosen_deployments"] = []
    state["available_products"] = {}
    state["available_archive_files"] = []
    state["product_checkboxes"] = {}
    state["archive_checkboxes"] = {}

    selected_options = change['new']
    if not selected_options:
        show_status("Device(s) unselected.", target='discover', working=False)
        _toggle_widgets(False)
        return

    parent_code = state["selected_parent_code"]
    details = state["parent_choice_details"].get(parent_code)
    onc_service = state.get("onc_service")
    all_params = state.get("all_params")

    if not details or not onc_service or not all_params:
        show_status("✖ Internal state error (missing details/client/params).", target='discover', error=True)
        _toggle_widgets(False)
        return

    all_parent_deployments = details['all_deployments']
    all_device_codes_sorted = details['device_codes']
    fake_args = type('obj', (object,), all_params)()

    # Determine chosen deployments
    if "ALL Hydrophones at this location" in selected_options:
        state["chosen_deployments"] = all_parent_deployments
        w_device_select.value = ("ALL Hydrophones at this location",)
    else:
        chosen_codes = set()
        for option_str in selected_options:
            match = re.search(r'\(([^)]+)\)$', option_str)
            if match:
                chosen_codes.add(match.group(1))
        state["chosen_deployments"] = [dep for dep in all_parent_deployments if dep.get('deviceCode') in chosen_codes]

    if not state["chosen_deployments"]:
        show_status("✖ No valid deployments found for selected device(s).", target='discover', error=True)
        _toggle_widgets(False)
        return

    first_chosen_dep = state["chosen_deployments"][0]
    first_device_code = first_chosen_dep.get("deviceCode")

    # Fetch Products or List Archive Files
    is_archive = (w_mode.value == 'Request Archived Data')
    ui_widgets_to_add = []

    try:
        if is_archive:
            show_status("Listing archive files...", target='discover', working=True)
            print("--- Listing Archive Files ---", file=sys.stderr)
            archive_filters = dict(
                deviceCode=first_device_code,
                dateFrom=utils.iso(all_params['start_dt'].astimezone(UTC)),
                dateTo=utils.iso(all_params['end_dt'].astimezone(UTC)),
                returnOptions='all'
            )
            list_result = onc_service.getArchivefile(filters=archive_filters, allPages=True)
            archive_files_info = list_result.get("files", [])
            state["available_archive_files"] = archive_files_info
            print(f"Found {len(archive_files_info)} archive file entries.", file=sys.stderr)
            ui_widgets_to_add = _build_archive_selection_ui(archive_files_info)
            w_archive_selection_area.children = tuple(ui_widgets_to_add)

        else:  # Data Product Mode
            show_status("Fetching available data products...", target='discover', working=True)
            print("--- Fetching Data Products ---", file=sys.stderr)
            prod_opts_raw = onc_service.getDataProducts({"deviceCode": first_device_code})
            available_data_products = defaultdict(list)
            if isinstance(prod_opts_raw, list):
                for p in prod_opts_raw:
                    if isinstance(p, dict) and p.get('extension'):
                        ext = p['extension'].lower()
                        if ext in settings.SUPPORTED_EXTENSIONS:
                            available_data_products[ext].append(p)
            state["available_products"] = available_data_products
            print(f"Found products for extensions: {list(available_data_products.keys())}", file=sys.stderr)
            ui_widgets_to_add = _build_product_selection_ui(available_data_products)
            w_product_selection_area.children = tuple(ui_widgets_to_add)

        if not ui_widgets_to_add:
            show_status("No products/files found to select.", target='discover', error=False)
        else:
            show_status("Device(s) selected. Select products/file types.", target='discover', working=False)
            w_download_btn.disabled = False

    except Exception as e:
        show_status(f"✖ Error fetching products/files: {e}", target='discover', error=True)
        logging.exception("Product/File Fetch Error:")
        w_product_selection_area.children = []
        w_archive_selection_area.children = []
    finally:
        _toggle_widgets(False)

def on_download_button_clicked(b):
    """Handles the 'Start Download / Process' button click."""
    print("Download button clicked!", file=sys.stderr)
    _toggle_widgets(True)
    _set_button_state(w_download_btn, working=True)
    show_status("Preparing download...", target='download', working=True)
    w_output_area.clear_output(wait=True)

    try:
        # --- Collect Final Parameters from STATE ---
        params = state['all_params'].copy()
        # Get required items from state, validate they exist
        params['onc_service'] = state.get('onc_service')
        params['chosen_deployments'] = state.get('chosen_deployments')

        if not params['onc_service'] or not params['chosen_deployments']:
            show_status("✖ State error: Discovery must complete successfully first.", target='download', error=True)
            _toggle_widgets(False); _set_button_state(w_download_btn, working=False); return

        is_archive = (w_mode.value == 'Request Archived Data')
        params['archive'] = is_archive
        # *** Override 'test' flag for the download step ***
        # If the user clicked the download button, they intend to download,
        # regardless of whether Archive mode was selected initially.
        params['test'] = False  # Force test=False for the actual run/download

        chosen_products_for_run = {}
        if not is_archive:
            # Collect selected data products
            for (prod_code, ext), cb in state.get("product_checkboxes", {}).items():
                if cb.value:
                    product_dict = next((p for p in state["available_products"].get(ext, []) if p.get('dataProductCode') == prod_code), None)
                    if product_dict:
                        if ext not in chosen_products_for_run: chosen_products_for_run[ext] = []
                        chosen_products_for_run[ext].append(product_dict)
                    else:
                        show_status(f"✖ Internal Error finding product {prod_code} ({ext})", target='download', error=True)
                        _toggle_widgets(False); _set_button_state(w_download_btn, working=False); return
        else:
            # Collect selected archive extensions
            selected_archive_exts = [ext for ext, cb in state.get("archive_checkboxes", {}).items() if cb.value]
            params['selected_archive_extensions'] = selected_archive_exts
            if 'flac' in selected_archive_exts:
                chosen_products_for_run['flac'] = [{'extension': 'flac', 'dataProductCode': 'ARCHIVE_FLAC'}]

        params['chosen_products'] = chosen_products_for_run

        # --- Final Validation ---
        if not is_archive and not params['chosen_products']:
            show_status("✖ No data products selected.", target='download', error=True)
            _toggle_widgets(False); _set_button_state(w_download_btn, working=False); return
        if is_archive and not params.get('selected_archive_extensions'):
             show_status("✖ No archive file types selected.", target='download', error=True)
             _toggle_widgets(False); _set_button_state(w_download_btn, working=False); return

        # --- Run the Download Logic ---
        with w_output_area:
            print("\n--- Starting Download/Processing ---")
            show_status("Processing request...", target='download', working=True)
            logger = logging.getLogger()
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
                handler.close()
            log_level = logging.DEBUG if params['debug'] else logging.INFO
            notebook_handler = core_downloader.OutputWidgetHandler(w_output_area)
            notebook_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(notebook_handler)
            logger.setLevel(log_level)

            # Call run_download_logic - it will use params['archive'] correctly
            # and params['test'] is now forced to False for this execution.
            exit_code = core_downloader.run_download_logic(params, output_widget=w_output_area)

            print("\n--- Process Finished ---")
            final_message = ""; final_is_error = False
            if exit_code == 0:
                final_message = "✅ Process completed successfully."
            elif exit_code == 2:
                final_message = "⚠️ Process cancelled by user."
                final_is_error = True
            else:
                final_message = "❌ Process finished with errors."
                final_is_error = True
            show_status(final_message, target='download', error=final_is_error)

    except Exception as e:
        with w_output_area:
            print(f"\n--- UNEXPECTED ERROR ---")
            traceback.print_exc()
        show_status(f"✖ Unexpected Error: {e}", target='download', error=True)
        logging.exception("Error during download button click:")
    finally:
        _toggle_widgets(False)  # Ensure UI is re-enabled
        _set_button_state(w_download_btn, working=False)  # Ensure button is idle

# --- Download Location Observer ---
def on_download_target_change(change):
    """Shows/hides path widgets based on download target selection."""
    target = change['new']
    if target == 'Google Drive':
        w_colab_path.layout.display = 'none'
        w_drive_mount_instruct.layout.display = 'block'
        
        # Handle Google Drive selection
        if FILECHOOSER_AVAILABLE:
            # Check if Drive is mounted before attempting to create FileChooser
            if COLAB_ENV and os.path.isdir(DRIVE_MYDRIVE_PATH):
                try:
                    # Create new FileChooser instance if needed
                    if state.get("drive_file_chooser_instance") is None:
                        fc = FileChooser(DRIVE_MYDRIVE_PATH)
                        fc.title = '<b>Select Google Drive Destination Folder</b>'
                        fc.show_only_dirs = True
                        state["drive_file_chooser_instance"] = fc
                    # Show the existing or new instance
                    w_download_drive_container.children = [state["drive_file_chooser_instance"]]
                except InvalidPathError:
                    # Handle path validation error
                    error_msg = widgets.HTML(
                        "<i style='color:red'>Error accessing Google Drive path. Please ensure Drive is mounted correctly.</i>"
                    )
                    w_download_drive_container.children = [error_msg]
            else:
                # Drive not mounted, show message
                mount_msg = widgets.HTML(
                    "<i style='color:orange'>Mount Google Drive first to enable folder picker.</i>"
                )
                w_download_drive_container.children = [mount_msg]
        else:
            # FileChooser not available, show manual input option
            manual_input = widgets.VBox([
                widgets.HTML("<b>Enter Google Drive Path:</b>"),
                widgets.Text(
                    description="Drive Path:",
                    value="",
                    placeholder="/content/drive/MyDrive/downloads",
                    layout=widgets.Layout(width='auto', min_width='300px')
                )
            ])
            w_download_drive_container.children = [manual_input]
    else:  # Colab Environment
        w_colab_path.layout.display = 'flex'
        w_download_drive_container.children = []  # Hide drive options
        w_drive_mount_instruct.layout.display = 'none'

# --- Mode Change Observer ---
def on_mode_change(change):
    """Handles mode changes between Data Product and Archive."""
    w_product_selection_area.children = []
    w_archive_selection_area.children = []
    w_product_archive_label.value = ""
    if w_location_select.value:
        w_location_select.value = None  # Trigger reset if location was selected
    else:
        # If location wasn't selected, manually clear downstream
        w_location_select.options = []
        w_device_select.options = []
        w_device_select.value = []
        w_download_btn.disabled = True
    clear_status()

# --- Widget Observation Setup ---
# (Keep existing observers + add new one)
w_discover_btn.on_click(on_discover_button_clicked)
w_location_select.observe(on_location_selected, names='value')
w_device_select.observe(on_device_selected, names='value')
w_download_btn.on_click(on_download_button_clicked)
w_mode.observe(on_mode_change, names='value')
w_download_target.observe(on_download_target_change, names='value')

# --- Main UI Display Function ---
def display_ui():
    """Arranges and displays the widgets."""
    clear_output()

    # Layout setup widgets
    setup_widgets = [
        widgets.HTML("<b>1. Setup Parameters:</b>"),
        w_token,
        w_start_hbox,
        w_end_hbox,
        widgets.HBox([w_tz]), # TZ on its own line maybe?
        w_download_location_box, # Add the location options box
        w_mode,
        w_opts_hbox
    ]
    setup_box = widgets.VBox(setup_widgets, layout=widgets.Layout(margin='0 0 10px 0'))

    # Layout discovery/selection widgets
    discovery_widgets = [
        w_discover_btn,
        w_discover_status_label, # Status below Discover button
        widgets.HTML("<br>"), # Add space
        w_location_label,
        w_location_select,
        w_device_label,
        w_device_select
    ]
    discovery_box = widgets.VBox(discovery_widgets, layout=widgets.Layout(margin='0 0 10px 0'))

    # Layout product/archive selection
    selection_widgets = [
        w_product_archive_label,
        w_product_selection_area,
        w_archive_selection_area
    ]
    selection_box = widgets.VBox(selection_widgets, layout=widgets.Layout(margin='0 0 10px 0'))

    # Layout download button & status
    download_widgets = [
        w_download_btn,
        w_download_status_label # Status below Download button
    ]
    download_box = widgets.VBox(download_widgets, layout=widgets.Layout(margin='0 0 10px 0'))

    # Combine sections
    ui_layout = widgets.VBox([
        setup_box,
        widgets.HTML("<hr>"),
        discovery_box,
        widgets.HTML("<hr>"),
        selection_box,
        widgets.HTML("<hr>"),
        download_box,
        widgets.HTML("<hr>"),
        widgets.HTML("<b>Log Output:</b>"),
        w_output_area
    ])

    # Trigger initial state for download location visibility
    on_download_target_change({'new': w_download_target.value})

    display(ui_layout)

# --- Entry point for use in Colab ---
# from hydrophone.notebook_ui import colab_ui
# colab_ui.display_ui() 