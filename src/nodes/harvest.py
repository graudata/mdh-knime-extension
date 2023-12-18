"""Harvest nodes."""

# Python imports
import logging
from pathlib import Path

# 3rd party imports
import knime.extension as knext

# Local imports
import mdh
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
from utils.paths import is_absolute_file_path

LOGGER = logging.getLogger(__name__)


__category = knext.category(
    path='/community/mdh',
    level_id='harvest',
    name='Harvest',
    description='MdH Harvest Nodes.',
    icon='icons/mdh.png',
)


####################
# Parameter Groups #
####################


@knext.parameter_group(label='Parameter')
class MetadataHarvestParameter:  # noqa[D101]

    input_task_file = knext.StringParameter(
        'Path to a harvest task configuration',
        'The absolute path to a file containing a valid GraphQL harvest task configuration.'
    )
    blocking = knext.BoolParameter(
        'Wait for task completion?',
        'Mark as checked if the node must wait until the harvest task is finished \
        before proceeding with downstream nodes.',
        default_value=True
    )


#################
# Harvest Ndess #
#################

@knext.node(
    name='Metadata Harvest',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/harvest.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
class HarvestNode(knext.PythonNode):
    """Run a harvest task on a MdH Core.

    By specifying a file containing a **GraphQL MdH Harvest Task Configuration**,
    a harvest task will be scheduled and eventually executed.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (harvestScheduleAdd) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.

    """

    parameter = MetadataHarvestParameter()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
    ):
        """Node configuration."""
        if not is_absolute_file_path(Path(self.parameter.input_task_file)):
            LOGGER.warning(f' {Messages.EXISTING_GRAPHQL_FILE}')
            config_context.set_warning(Messages.EXISTING_GRAPHQL_FILE)

        return None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        mdh_connection: MdHInstanceConnectionPortObject
    ):
        """Node execution."""
        instance = mdh_connection.data[FlowVariables.INSTANCE]

        is_global_search = mdh_instance_is_global_search(instance)
        if is_global_search:
            raise RuntimeError(Messages.HARVEST_GLOBAL_SEARCH)

        # Note: is_global_search == False
        if not mdh_instance_is_running(instance, is_global_search):
            raise RuntimeError(
                Messages.ADD_RUNNING_CORE_BY_NAME.format(core=instance)
            )
        if not is_absolute_file_path(Path(self.parameter.input_task_file)):
            raise RuntimeError(Messages.EXISTING_GRAPHQL_FILE)

        task_id = \
            mdh.core.harvest.schedule_add(instance, Path(self.parameter.input_task_file))

        if self.parameter.blocking:
            exec_context.set_progress(
                0.5,
                message='Waiting for task completion (This might take a while)'
            )
            mdh.core.harvest.active_wait(instance, task_id)

        return None
