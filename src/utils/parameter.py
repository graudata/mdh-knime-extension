"""Parameter specific utils."""

# Python imports
from typing import Any, Final


# 3rd party imports
import knime.extension as knext


class FlowVariables:
    """Supported flow variables."""

    INSTANCE            : Final[str] = 'instance'
    IS_GLOBAL_SEARCH    : Final[str] = 'is_global_search'
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


def get_parameter_or_flow_variable(
    candidate_variable: str | bool | None,
    flow_variable: str,
    context: knext.ConfigurationContext
) -> Any:
    """Select the parameter from the flow variable context or use the candidate variable.

    param: candidate_variable: candidate for parameter selection
    param: flow_variable: the overwriting flow variable
    param: context: flow variable context
    """
    try:
        variable = context.flow_variables[flow_variable]
    except KeyError:
        variable = candidate_variable

    if variable is None:
        raise ValueError(f'Please provide a valid variable \'{flow_variable}\'')

    return variable
