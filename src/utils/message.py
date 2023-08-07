"""Message specific utils."""

# Python imports
from typing import Final


class Messages:
    """Available node/log messages."""

    # Misc
    ADD_RUNNING_INSTANCE        : Final[str] = 'Please add a running MdH Instance'
    ADD_RUNNING_CORE            : Final[str] = 'Please add a running MdH Core'
    ADD_RUNNING_GLOBAL_SEARCH   : Final[str] = 'Please add a running MdH Global Search'
    EXISTING_GRAPHQL_FILE       : Final[str] = 'Please provide an existing GraphQl file'

    # Harvest
    HARVEST_GLOBAL_SEARCH       : Final[str] = 'Tasks cannot be executed on a MdH Global Search'

    # Query
    QUERY_VALID_INPUT_FILE      : Final[str] = \
        'Please provide a valid file to retrieve metadata from'
    QUERY_VALID_DOWNLOAD_FORMAT : Final[str] = 'Please provide a valid download format'
    QUERY_VALID_OUTPUT_FILE     : Final[str] = \
        'Please provide an absolute file path for the output file'
