# Hydrophone Data Downloader for Ocean Networks Canada

A Python tool for downloading and processing hydrophone data from Ocean Networks Canada (ONC). This project provides both a command-line interface and a Google Colab UI for accessing hydrophone data, calibration information, and related products.

## Features

- Download hydrophone data in multiple formats (WAV, FLAC, PNG, TXT)
- Fetch hydrophone calibration data
- Support for both Data Product and Archive modes
- Interactive selection of locations and devices
- Progress tracking and detailed logging
- Google Colab integration with interactive UI
- Timezone-aware date/time handling
- Robust error handling and retry mechanisms

## Installation

### Prerequisites

The project requires Python 3.7 or later. All dependencies are listed in `requirements.txt`. The main requirements are:

- `requests`: For HTTP interactions with the ONC API
- `python-dateutil`: For timezone and date handling
- `onc-python`: Official ONC API client
- `ipywidgets`: For interactive UI components
- `pytz`: For timezone support
- `ipyfilechooser`: Optional, for Google Drive integration in Colab

### Installation Steps

1. Clone the repository:
```bash
git clone https://github.com/Spiffical/hydrophonedatarequests.git
cd hydrophonedatarequests
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
```

For Google Colab usage, you can install dependencies directly in your notebook:
```python
!pip install requests python-dateutil onc-python ipywidgets pytz ipyfilechooser
```

## Usage

### Command Line Interface

```bash
hydrophone-dl [options]
```

Common options:
- `--token`: Your ONC API token
- `--start`: Start date/time
- `--end`: End date/time
- `--timezone`: Timezone for date/time interpretation
- `--output`: Output directory
- `--archive`: Use Archive/Test mode
- `--fetch-sensitivity`: Download calibration data
- `--debug`: Enable debug logging

### Google Colab Interface

```python
from hydrophone.notebook_ui import colab_ui
colab_ui.display_ui()
```

The Colab UI provides an interactive interface with:
- Token input
- Date/time selection with timezone support
- Location and device selection
- Data product/archive file type selection
- Download location options (Colab environment or Google Drive)
- Progress tracking and status updates

## Data Products

The tool supports downloading various data products:

1. **Data Product Mode**:
   - PNG spectrograms
   - TXT format data
   - Other supported extensions defined in settings

2. **Archive Mode**:
   - FLAC audio files
   - Raw data archives
   - Associated metadata

## Calibration Data

When enabled (`--fetch-sensitivity` or UI checkbox), the tool can fetch:
- Hydrophone sensitivity vectors
- Frequency bin calibration data
- Validity date ranges
- Calibration metadata

## Project Structure

```
src/
├── bin/
│   └── hydrophone-dl          # CLI entry point
├── hydrophone/
│   ├── core/
│   │   ├── calibration.py     # Calibration data handling
│   │   ├── downloader.py      # Main download logic
│   │   └── onc_client.py      # ONC API interaction
│   ├── notebook_ui/
│   │   └── colab_ui.py        # Google Colab interface
│   ├── utils/
│   │   ├── helpers.py         # Utility functions
│   │   └── exceptions.py      # Custom exceptions
│   └── config/
│       └── settings.py        # Configuration settings
```

## Error Handling

The tool includes comprehensive error handling for:
- Network issues
- API permissions
- Invalid data
- Missing files
- Timeout conditions
- User interruptions

## Contributing

Contributions are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.

## License

...

## Acknowledgments

- Ocean Networks Canada for providing the data and API
- Contributors to the onc-python library

## Support

For issues, questions, or contributions, please:
1. Check the existing issues
2. Create a new issue with a detailed description
3. Include relevant logs and error messages

