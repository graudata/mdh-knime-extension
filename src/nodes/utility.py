"""Utility nodes."""

# Python imports
import json
import logging

# 3rd party imports
import knime.extension as knext
import pandas as pd

# Local imports
import mdh
from ports.instance_connection import (
    INSTANCE_CONNECTION_TYPE,
    MdHInstanceConnectionPortObject,
)
from utils.mdh import (  # noqa[I100,I201]
    get_global_search_headers,
    mdh_instance_is_global_search,
    mdh_instance_is_running
)
from utils.message import Messages
from utils.parameter import FlowVariables


LOGGER = logging.getLogger(__name__)


__category = knext.category(
    path='/community/mdh',
    level_id='utility',
    name='Utility',
    description='MdH Utility Nodes.',
    icon='icons/mdh.png',
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
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.output_table(
    'info table',
    'A KNIME table with one column containing the queried dashboard information as a JSON string.'
)
class InfoNode(knext.PythonNode):
    """Get dashboard information of a running MdH Core instance.

    Get dashboard information of a running MdH Core instance.
    """

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
            result = mdh.global_search.main.info(
                instance,
                get_global_search_headers(mdh_connection.data)
            )
        else:
            result = mdh.core.main.info(instance)

        info = json.dumps(result)
        df = pd.DataFrame(
            {
                'info': info
            },
            index=[0],
        )
        return knext.Table.from_pandas(df)
