"""Utility nodes."""

# Python imports
import json
import logging

# 3rd party imports
import knime.extension as knext
import pandas as pd

# Local imports
import mdh
from utils.mdh import (  # noqa[I100,I201]
    mdh_instance_is_global_search,
    mdh_instance_is_running,
    update_global_search_args
)
from utils.message import Messages


LOGGER = logging.getLogger(__name__)


__category = knext.category(
    path='/community/mdh',
    level_id='utility',
    name='Utility',
    description='MdH Utility Nodes.',
    icon='icons/mdh.png',
)


####################
# Parameter Groups #
####################


@knext.parameter_group(label='Instance configuration')
class Instance:  # noqa[D101]

    name = knext.StringParameter(
        'Instance name',
        'The name of a MdH Core or Global Search to retrieve dashboard information from.',
    )


#################
# Utility Nodes #
#################


@knext.node(
    name='Dashboard Information',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/mdh.png',
    category=__category
)
@knext.output_table(
    'info table',
    'A KNIME table with one column containing the queried dashboard information as a JSON string.'
)
class InfoNode(knext.PythonNode):
    """Get dashboard information of a running MdH Core instance.

    Get dashboard information of a running MdH Core instance.
    """

    instance = Instance()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
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
            func = mdh.global_search.main.info
        else:
            func = mdh.core.main.info

        kwargs: dict[str, str | list[str]] = {}
        if is_global_search:
            update_global_search_args(kwargs, exec_context.flow_variables)

        result = func(
            self.instance.name,
            **kwargs
        )

        info = json.dumps(result)
        df = pd.DataFrame(
            {
                'info': info
            },
            index=[0],
        )
        return knext.Table.from_pandas(df)
