"""Statistic nodes."""

# Python imports
import contextlib
import logging
from types import ModuleType
from typing import Callable

# 3rd party imports
import knime.extension as knext
import pandas as pd

# Local imports
import mdh
from mdh.errors import MdHApiError
from mdh.types.connection import GlobalSearchHeaders
from mdh.types.statistic import (
    StatisticFileTypesParameters,
    StatisticMetadataTagsParameters,
    StatisticMimeTypesParameters
)
from ports.instance_connection import (
    INSTANCE_CONNECTION_TYPE,
    MdHInstanceConnectionPortObject,
    MdHInstanceConnectionPortObjectSpec
)
from utils.mdh import (  # noqa[I100,I201]
    mdh_instance_is_global_search,
    mdh_instance_is_running
)
from utils.message import Messages
from utils.parameter import FlowVariables

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path='/community/mdh',
    level_id='statistics',
    name='Statistics',
    description='MdH Statistic Nodes.',
    icon='icons/mdh.png',
)

####################
# Parameter Groups #
####################


class StatisticOptions(knext.EnumParameterOptions):  # noqa[D101]
    FILETYPE = (
        'filetype',
        'Statistical information about harvested file types, \
        e.g. number of occurences of a specific file'
    )
    MIMETYPE = (
        'mimetype',
        'Statistical information about harvested MIME types, \
        e.g. number of occurences of a specific MIME type (without subtype)'
    )
    METADATA = (
        'metadata',
        'Statistical information about harvested metadata, \
        e.g. number of occurences of a specific tag'
    )


@knext.parameter_group(label='Parameter')
class MetadataStatisticParameter:  # noqa[D101]

    statistic_selection_param = knext.EnumParameter(
        'Statistic Option',
        'Choose one of the statistic options.',
        default_value=StatisticOptions.FILETYPE.name,  # type: ignore[attr-defined]
        enum=StatisticOptions
    )
    limit = knext.IntParameter(
        'Limit',
        'Adjust the maximum of retrieved entries. A value of zero returns all available entries.',
        min_value=0,
        is_advanced=True
    )


###################
# Statistic Nodes #
###################


@knext.node(
    name='Metadata Statistics',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/read.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.output_table(
    'statistics table',
    'A KNIME table with one column containing the aggregated statistics as a JSON string.'
)
class StatisticNode(knext.PythonNode):
    """Retrieve statistical information from a MdH Core or Global Search.

    Retrieve statistical information from a MdH Core or Global Search about harvested metadata.
    """

    parameter = MetadataStatisticParameter()

    def _get_statistic_method_and_param_type(
        self,
        statistic_module: ModuleType,
        selection_param: str
    ) -> tuple[
        Callable,
        StatisticFileTypesParameters |
        StatisticMimeTypesParameters |
        StatisticMetadataTagsParameters
    ]:
        """Get the method and parameter type of corresponding statistics module."""
        if (selection_param == StatisticOptions.FILETYPE.name):  # type: ignore[attr-defined]
            return (statistic_module.get_filetype, StatisticFileTypesParameters)
        if (selection_param == StatisticOptions.MIMETYPE.name):  # type: ignore[attr-defined]
            return (statistic_module.get_mimetype, StatisticMimeTypesParameters)
        return (statistic_module.get_metadata, StatisticMetadataTagsParameters)

    def _get_statistic_data(
        self,
        result: list[dict],
        selection_param: str
    ):
        def try_append(column: list, entry: dict, key: str) -> None:
            with contextlib.suppress(KeyError):
                column.append(entry[key])
        type_ = []
        file_count = []
        metadata_count = []
        tag_count = []
        data_type = []
        space = []
        metadata_count_aggregated_values = []

        for entry in result:
            try_append(type_, entry, 'name')
            try_append(file_count, entry, 'fileCount')
            try_append(data_type, entry, 'type')
            try_append(tag_count, entry, 'count')
            try_append(metadata_count, entry, 'metadataCount')
            try_append(space, entry, 'space')
            try_append(metadata_count_aggregated_values, entry, 'metadataCountAggregatedValues')

        data = {}
        if (selection_param == StatisticOptions.FILETYPE.name):  # type: ignore[attr-defined]
            data['FileType'] = type_
            data['MetadataCount'] = metadata_count
            data['MetadataCountAggregatedValues'] = metadata_count_aggregated_values
            data['FileCount'] = file_count
            data['Space (in bytes)'] = space
        elif (selection_param == StatisticOptions.MIMETYPE.name):  # type: ignore[attr-defined]
            data['MIMEType'] = type_
            data['FileCount'] = file_count
            data['Space (in bytes)'] = space
        else:
            data['Tag'] = type_
            data['DataType'] = data_type
            data['TagCount'] = tag_count

        return data

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: MdHInstanceConnectionPortObjectSpec
    ):
        """Node configuration."""
        return None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        mdh_connection: MdHInstanceConnectionPortObject
    ):
        """Node execution."""
        instance = mdh_connection.data[FlowVariables.INSTANCE]

        is_global_search = mdh_instance_is_global_search(instance)
        if not mdh_instance_is_running(instance, is_global_search):
            raise RuntimeError(
                Messages.ADD_RUNNING_INSTANCE_BY_NAME.format(instance=instance)
            )

        if is_global_search:
            statistic_module = mdh.global_search.statistic
        else:
            statistic_module = mdh.core.statistic

        func, parameters = self._get_statistic_method_and_param_type(
            statistic_module,
            self.parameter.statistic_selection_param
        )
        limit = self.parameter.limit if self.parameter.limit >= 1 else None

        try:
            if is_global_search:
                result = func(
                    instance,
                    parameters=parameters(limit=limit),
                    global_search_headers=GlobalSearchHeaders(
                        mdh_connection.data[FlowVariables.SELECTED_CORES],
                        mdh_connection.data[FlowVariables.IGNORE_ERRORS],
                        mdh_connection.data[FlowVariables.IGNORE_FAILED_CONS]
                    )
                )
            else:
                result = func(
                    instance,
                    parameters=parameters(limit=limit)
                )
        except MdHApiError as err:
            LOGGER.error(str(err))
            raise

        data = self._get_statistic_data(
            result,
            self.parameter.statistic_selection_param
        )

        return knext.Table.from_pandas(
            pd.DataFrame(data)
        )
