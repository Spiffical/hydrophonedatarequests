# hydro_dl/config.py

# Standard ISO format string used by ONC API
ISO_FMT = "%Y-%m-%dT%H:%M:%S.000Z"

# Default ONC API timeout
DEFAULT_ONC_TIMEOUT = 60

# Default parameters for specific product types
PNG_DEFAULT_PARAMS = dict(dpo_lowerColourLimit=-1000, dpo_upperColourLimit=-1000)
WAV_DEFAULT_PARAMS = {
        'dpo_audioFormatConversion': 0,  # 0 = No conversion (usually)
        'dpo_audioDownsample': -1      # -1 = No downsampling (usually)
    }

# Supported file extensions for download
SUPPORTED_EXTENSIONS = ("flac", "png", "txt")

# Fallback download settings (can be overridden by args)
DEFAULT_FALLBACK_RETRIES = 12
DEFAULT_FALLBACK_WAIT_SECONDS = 5.0