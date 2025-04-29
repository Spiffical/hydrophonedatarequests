# hydro_dl/exceptions.py

class HydroDLError(Exception):
    """Base exception for hydro_dl application errors."""
    pass

class ConfigError(HydroDLError):
    """Errors related to configuration (token, dates, timezone)."""
    pass

class ONCInteractionError(HydroDLError):
    """Errors during interaction with the ONC API."""
    pass

class UserAbortError(HydroDLError):
    """Exception raised when the user aborts an operation."""
    pass

class NoDataError(HydroDLError):
    """Exception raised when no suitable data or deployments are found."""
    pass

class DownloadError(HydroDLError):
    """Errors specifically during the download phase."""
    pass