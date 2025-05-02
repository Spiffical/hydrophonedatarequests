"""
Configuration settings for the hydrophone data retrieval system.
"""

# Standard ISO format string used by ONC API
ISO_FMT = "%Y-%m-%dT%H:%M:%S.000Z"

# Default ONC API timeout (seconds)
DEFAULT_ONC_TIMEOUT = 60

# Default parameters for specific product types
PNG_DEFAULT_PARAMS = dict(dpo_lowerColourLimit=-1000, dpo_upperColourLimit=-1000)

# Supported file extensions and their corresponding data products:
# ACC: Hydrophone Acceleration Data (HACC)
# AN: Annotation File (AF)
# CSV: Time Series Scalar Data (TSSD)
# FFT: Hydrophone Spectral Data (HSD)
# FLAC: Audio Data (AD)
# JSON: Time Series Scalar Data (TSSD)
# MAT: Hydrophone Spectral Data (HSD), Spectral Probability Density (HSPD), Time Series Scalar Data (TSSD)
# PDF: Hydrophone Spectral Data (HSD), Spectral Probability Density (HSPD), Time Series Plots (TSSCP, TSSP)
# PNG: Hydrophone Spectral Data (HSD), Spectral Probability Density (HSPD), Spectrogram (SHV), Time Series Plots
# TXT: Log File (LF), Time Series Scalar Data (TSSD)
# Note: WAV/MP3 files are no longer supported by ONC
SUPPORTED_EXTENSIONS = ("acc", "an", "csv", "fft", "flac", "json", "mat", "pdf", "png", "txt")

# Fallback download settings (can be overridden by args)
DEFAULT_FALLBACK_RETRIES = 12
DEFAULT_FALLBACK_WAIT_SECONDS = 5.0

# Data Product Option mappings for different product types and extensions
# The key is a tuple of (product_code, extension) and the value is a dictionary of DPO parameters
DPO_MAPPINGS = {
    # Time Series Scalar Data - Temporarily removed all parameters to test
    ('TSSD', 'csv'): {},
    ('TSSD', 'json'): {},
    ('TSSD', 'mat'): {},
    ('TSSD', 'txt'): {},
    
    # Time Series Staircase Plot - Updated parameters
    ('TSSCP', 'pdf'): {
        'dpo_dataGaps': 1,
        'dpo_qualityControl': 1,
        'dpo_plotType': 'staircase',  # Add plot type specification
        'dpo_plotTitle': 'Time Series Staircase',  # Add plot title
        'dpo_plotSize': 'default'  # Add plot size parameter
    },
    ('TSSCP', 'png'): {
        'dpo_dataGaps': 1,
        'dpo_qualityControl': 1,
        'dpo_plotType': 'staircase',  # Add plot type specification
        'dpo_plotTitle': 'Time Series Staircase',  # Add plot title
        'dpo_plotSize': 'default'  # Add plot size parameter
    },
    
    # Time Series Scalar Plot - Temporarily removed all parameters to test
    ('TSSP', 'pdf'): {},
    ('TSSP', 'png'): {},
    
    # Hydrophone Spectral Data
    ('HSD', 'fft'): {},  # No DPO needed
    ('HSD', 'mat'): {
        'dpo_spectrogramConcatenation': 'Concatenate',  # Required for MAT
        'dpo_spectralDataDownsample': 1  # Required based on warning
    },
    ('HSD', 'pdf'): {
        'dpo_spectrogramColourPalette': 0,
        'dpo_spectrogramConcatenation': 'None',
        'dpo_spectrogramFrequencyUpperLimit': -1,
        'dpo_upperColourLimit': -1000,
        'dpo_lowerColourLimit': -1000
    },
    ('HSD', 'png'): {
        'dpo_spectrogramColourPalette': 0,
        'dpo_spectrogramConcatenation': 'None',
        'dpo_spectrogramFrequencyUpperLimit': -1,
        'dpo_upperColourLimit': -1000,
        'dpo_lowerColourLimit': -1000
    },
    
    # Hydrophone Spectral Probability Density
    ('HSPD', 'mat'): {
        'dpo_filePlotBreaks': 2
    },
    ('HSPD', 'pdf'): {
        'dpo_filePlotBreaks': 2,
        'dpo_spectralProbabilityDensityColourAxisUpperLimit': 0,
        'dpo_spectralProbabilityDensityPSDRange': 0
    },
    ('HSPD', 'png'): {
        'dpo_filePlotBreaks': 2,
        'dpo_spectralProbabilityDensityColourAxisUpperLimit': 0,
        'dpo_spectralProbabilityDensityPSDRange': 0
    },
    ('HACC', 'acc'): {},
    ('AF', 'an'): {}, 
    ('LF', 'txt'): {},
}