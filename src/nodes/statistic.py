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
from utils.mdh import (  # noqa[I100,I201]
    mdh_instance_is_global_search,
    mdh_instance_is_running,
    update_global_search_args
)
from utils.message import Messages
from utils.parameter import (  # noqa[I100,I201]
    FlowVariables,
    get_parameter_or_flow_variable
)


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


@knext.parameter_group(label='Instance configuration')
class Instance:  # noqa[D101]

    name = knext.StringParameter(
        'Instance name',
        'The name of a MdH Core or Global Search to run the GraphQL query on.',
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
@knext.output_table(
    'statistics table',
    'A KNIME table with one column containing the aggregated statistics as a JSON string.'
)
class StatisticNode(knext.PythonNode):
    """Retrieve statistical information from a MdH Core or Global Search.

    Retrieve statistical information from a MdH Core or Global Search about harvested metadata.
    """

    instance = Instance()
    parameter = MetadataStatisticParameter()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
        self.instance.name = get_parameter_or_flow_variable(
            self.instance.name,
            FlowVariables.INSTANCE,
            config_context
        )
        self.parameter.statistic_selection_param = get_parameter_or_flow_variable(
            self.parameter.statistic_selection_param,
            FlowVariables.STATISTIC_SELECTION,
            config_context
        )
        self.parameter.limit = get_parameter_or_flow_variable(
            self.parameter.limit,
            FlowVariables.LIMIT,
            config_context
        )

        is_global_search = mdh_instance_is_global_search(self.instance.name)
        if not mdh_instance_is_running(self.instance.name, is_global_search):
            LOGGER.warning(f' {Messages.ADD_RUNNING_INSTANCE}')
            config_context.set_warning(Messages.ADD_RUNNING_INSTANCE)

        return None

    def execute(self, exec_context: knext.ExecutionContext):
        """Node execution."""
        if not self.instance.name:
            raise RuntimeError(Messages.ADD_RUNNING_INSTANCE)

        is_global_search = mdh_instance_is_global_search(self.instance.name)
        if not mdh_instance_is_running(self.instance.name, is_global_search):
            raise RuntimeError(
                f'Please add a running MdH Instance called \'{self.core}\''
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
                kwargs: dict[str, str | list[str]] = {}
                update_global_search_args(kwargs, exec_context.flow_variables)
                result = func(
                    self.instance.name,
                    parameters=parameters(limit=limit),
                    global_search_headers=GlobalSearchHeaders(**kwargs)
                )
            else:
                result = func(
                    self.instance.name,
                    parameters=parameters(limit=limit)
                )
        except MdHApiError as err:
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
