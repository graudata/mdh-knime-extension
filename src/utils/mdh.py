"""MdH specific utils."""

# Python imports
import logging
import re

# Local imports
import mdh
from mdh.errors import (
    MdHEnvironmentError,
    MdHNotInitializedError
)
from mdh.types import (
    DownloadFormat,
    GlobalSearchHeaders
)
from .parameter import FlowVariables


LOGGER = logging.getLogger(__name__)


def get_running_mdh_core_names() -> list[str]:
    """Get names of running MdH Core Instances."""
    try:
        cores = mdh.core.main.get(only_running=True)  # type: ignore[attr-defined]
    except (MdHNotInitializedError, MdHEnvironmentError) as err:
        LOGGER.error(str(err))
    else:
        return [core.basic_info.name for core in cores]
    return []


def get_running_mdh_global_search_names() -> list[str]:
    """Get names of running MdH Global Search Instances."""
    try:
        global_searches = \
            mdh.global_search.main.get(only_running=True)  # type: ignore[attr-defined]
    except (MdHNotInitializedError, MdHEnvironmentError) as err:
        LOGGER.error(str(err))
    else:
        return [global_search.basic_info.name for global_search in global_searches]
    return []


def mdh_instance_is_running(name: str, is_global_search: bool) -> bool:
    """Check if the MdH Instance is running.

    :param name: the name of the MdH Instance
    :param is_global_search: if the MdH Instance is a `Global Search`

    """
    if is_global_search:
        func = get_running_mdh_global_search_names  # type: ignore[attr-defined]
    else:
        func = get_running_mdh_core_names  # type: ignore[attr-defined]

    return any(instance == name for instance in func())


def mdh_instance_is_global_search(name: str) -> bool:
    """Check if the MdH Instance is a `Global Search`.

    :param name: the name of the MdH Instance
    """
    try:
        global_searches = mdh.global_search.main.get()
    except (MdHNotInitializedError, MdHEnvironmentError) as err:
        raise RuntimeError(str(err))

    return any(global_search.basic_info.name == name for global_search in global_searches)


def mdh_download_format_exists(download_format: str) -> bool:
    """Check if the download format is supported by MdH.

    :param download_format: download format
    """
    return any(
        e.value == download_format for e in DownloadFormat  # type: ignore[attr-defined]
    )


def get_global_search_headers(
    flow_variables: dict[str, str]
) -> GlobalSearchHeaders:
    """Get global search header object from flow variables.

    :param header: Global search header dict
    """
    return GlobalSearchHeaders(
        flow_variables[FlowVariables.SELECTED_CORES],
        flow_variables[FlowVariables.IGNORE_ERRORS],
        flow_variables[FlowVariables.IGNORE_FAILED_CONS]
    )


def split_global_search_cores(cores: str) -> list[str]:
    """Split the comma seperated string of MdH Cores.

    :param cores: comma seperated MdH Cores
    :raises RuntimeError: if no MdH Core candidate string could be extracted
    """
    cores = [
        core
        for core in re.split(r'[^a-zA-Z0-9-_]+', cores)
        if core != ''
    ]
    if not cores:
        raise RuntimeError('Please provide a minimum of one MdH Core Instance')
    return cores
