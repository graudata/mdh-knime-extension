"""Statistic nodes."""

# Python imports
import json
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

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
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

        func, parameters = _get_statistic_method_and_param_type(
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
                        mdh_connection.data['cores'],
                        mdh_connection.data['ignore_errors'],
                        mdh_connection.data['ignore_failed_connections']
                    )
                )
            else:
                result = func(
                    instance,
                    parameters=parameters(limit=limit)
                )
        except MdHApiError as err:
            # TODO: Why errors on filetype and mimetype, but not metadata statistics
            # TODO: Is this note still relevant?
            # NOTE: Dirty workaround for empty cores in context of global search
            LOGGER.warning(str(err))
            result = []

        statistics = json.dumps(result)
        df = pd.DataFrame(
            {
                'statistics': statistics
            },
            index=[0],
        )
        return knext.Table.from_pandas(df)


def _get_statistic_method_and_param_type(
    statistic_module: ModuleType,
    selection_param: str,
) -> tuple[Callable,
           StatisticFileTypesParameters |
           StatisticMimeTypesParameters |
           StatisticMetadataTagsParameters]:
    """Get the method and parameter type of corresponding statistics module."""
    if (selection_param == StatisticOptions.FILETYPE.name):  # type: ignore[attr-defined]
        return (statistic_module.get_filetype, StatisticFileTypesParameters)
    if (selection_param == StatisticOptions.MIMETYPE.name):  # type: ignore[attr-defined]
        return (statistic_module.get_mimetype, StatisticMimeTypesParameters)
    return (statistic_module.get_metadata, StatisticMetadataTagsParameters)
