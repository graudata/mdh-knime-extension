"""Message specific utils."""

# Python imports
from typing import Final


class Messages:
    """Available node/log messages."""

    # Misc
    ADD_RUNNING_INSTANCE_BY_NAME          : Final[str] = \
        'Please add a running MdH Instance called \'{instance}\''
    ADD_RUNNING_CORE                  : Final[str] = 'Please add a running MdH Core'
    ADD_RUNNING_CORE_BY_NAME          : Final[str] = \
        'Please add a running MdH Core called \'{core}\''
    ADD_RUNNING_GLOBAL_SEARCH         : Final[str] = 'Please add a running MdH Global Search'
    ADD_RUNNING_GLOBAL_SEARCH_BY_NAME : Final[str] = \
        'Please add a running MdH Global Search called \'{global_search}\''

    # Query
    QUERY_VALID_DOWNLOAD_FORMAT       : Final[str] = 'Please provide a valid download format'
    QUERY_VALID_OUTPUT_FILE           : Final[str] = \
        'Please provide an absolute file path for the output file'
    QUERY_VALID_FILTER_LOGIC                : Final[str] = \
        '{filter_key} must be part of \'{filter_logic}\''
