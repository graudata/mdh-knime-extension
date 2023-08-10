"""Parameter specific utils."""

# Python imports
from typing import Final


class FlowVariables:
    """Supported flow variables."""

    INSTANCE            : Final[str] = 'instance'
    IGNORE_ERRORS       : Final[str] = 'ignore_errors'
    IGNORE_FAILED_CONS  : Final[str] = 'ignore_failed_connections'
    ONLY_COUNT          : Final[str] = 'only_count'
    INPUT_METADATA_FILE : Final[str] = 'input_metadata_file'
    INPUT_QUERY_FILE    : Final[str] = 'input_query_file'
    INPUT_TASK_FILE     : Final[str] = 'input_task_file'
    DOWNLOAD_FORMAT     : Final[str] = 'download_format'
    OUTPUT_RESULT_FILE  : Final[str] = 'output_result_file'
    BLOCKING            : Final[str] = 'blocking'
    LIMIT               : Final[str] = 'limit'
    STATISTIC_SELECTION : Final[str] = 'statistic_selection'
    SELECTED_CORES      : Final[str] = 'selected_cores'
