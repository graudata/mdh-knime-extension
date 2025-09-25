"""Parameter specific utils."""

# Python imports
from typing import Final


class FlowVariables:
    """Supported flow variables."""

    INSTANCE            : Final[str] = 'instance'
    IGNORE_ERRORS       : Final[str] = 'ignore_errors'
    IGNORE_FAILED_CONS  : Final[str] = 'ignore_failed_connections'
    QUERY               : Final[str] = 'query'
    SELECTED_CORES      : Final[str] = 'selected_cores'
