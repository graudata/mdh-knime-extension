"""Configuration nodes."""

# Python imports
import logging

# 3rd party imports
import knime.extension as knext

# Local imports
import mdh
from ports.instance_connection import (
    INSTANCE_CONNECTION_TYPE,
    MdHInstanceConnectionPortObject,
    MdHInstanceConnectionPortObjectSpec
)
from utils.mdh import (  # noqa[I100,I201]
    get_running_mdh_core_names,
    get_running_mdh_global_search_names,
    mdh_instance_is_running,
    split_global_search_cores
)
from utils.message import Messages
from utils.parameter import FlowVariables


LOGGER = logging.getLogger(__name__)


__category = knext.category(
    path='/community/mdh',
    level_id='config',
    name='Instance Selection',
    description='MdH Instance Selection Nodes.',
    icon='icons/mdh.png',
)


####################
# Parameter Groups #
####################


@knext.parameter_group(label='Instance Selection')
class CoreInstance:  # noqa[D101]

    core = knext.StringParameter(
        'MdH Core Instances',
        'Select a MdH Core Instance.',
        enum=get_running_mdh_core_names()
    )


@knext.parameter_group(label='Instance Selection')
class GlobalSearchInstance:  # noqa[D101]

    global_search = knext.StringParameter(
        'MdH Global Search Instances',
        'Select a MdH Global Search Instance.',
        enum=get_running_mdh_global_search_names()
    )
    selected_cores = knext.StringParameter(
        'Included MdH Core Instances',
        'Provide a comma seperated list of included MdH Core Instances',
        default_value=', '.join(core for core in get_running_mdh_core_names())
    )


@knext.parameter_group(label='Error Handling', is_advanced=True)
class ErrorHandling:  # noqa[D101]

    ignore_errors = knext.BoolParameter(
        'Ignore errors',
        'Ignore errors of single MdH Core instances',
        default_value=False,
    )
    ignore_failed_connections = knext.BoolParameter(
        'Ignore failed connections',
        'Ignore failed connections to MdH Core instances',
        default_value=False,
    )


#######################
# Configuration Nodes #
#######################

@knext.node(
    name='MdH Core Selection',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/config.png',
    category=__category
)
@knext.output_port(
    name='Output port',
    description='Connection data for MdH nodes',
    port_type=INSTANCE_CONNECTION_TYPE
)
class CoreConfigurationNode(knext.PythonNode):
    """Select a running MdH Core instance.

    This node retrieves a list of running **MdH Core** instances based on your MdH environment
    and provides downstream MdH nodes with connection data.
    """

    instance = CoreInstance()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
        cores = mdh.core.main.get(only_running=True)
        if not cores:
            LOGGER.warning(f' {Messages.ADD_RUNNING_CORE}')
            config_context.set_warning(Messages.ADD_RUNNING_CORE)

        return MdHInstanceConnectionPortObjectSpec(INSTANCE_CONNECTION_TYPE.id)

    def execute(self, exec_context: knext.ExecutionContext):
        """Node execution."""
        if not self.instance.core:
            raise RuntimeError(Messages.ADD_RUNNING_CORE)
        if not mdh_instance_is_running(self.instance.core, False):
            raise RuntimeError(
                Messages.ADD_RUNNING_CORE_BY_NAME.format(core=self.instance.core)
            )

        return MdHInstanceConnectionPortObject(
            MdHInstanceConnectionPortObjectSpec(INSTANCE_CONNECTION_TYPE.id),
            {
                FlowVariables.INSTANCE: self.instance.core
            }
        )


@knext.node(
    name='MdH Global Search Selection',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/config.png',
    category=__category
)
@knext.output_port(
    name='Output port',
    description='Connection data for MdH nodes',
    port_type=INSTANCE_CONNECTION_TYPE
)
class GlobalSearchConfigurationNode(knext.PythonNode):
    """Select a running MdH Global Search instance.

    This node retrieves a list of running **MdH Global Search** instances based on your MdH environment
    and provides downstream MdH nodes with connection data.
    """

    instance = GlobalSearchInstance()
    error_behavior = ErrorHandling()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
        global_searches = mdh.global_search.main.get(only_running=True)
        if not global_searches:
            LOGGER.warning(f' {Messages.ADD_RUNNING_GLOBAL_SEARCH}')
            config_context.set_warning(Messages.ADD_RUNNING_GLOBAL_SEARCH)

        return MdHInstanceConnectionPortObjectSpec(INSTANCE_CONNECTION_TYPE.id)

    def execute(self, exec_context: knext.ExecutionContext):
        """Node execution."""
        if not self.instance.global_search:
            raise RuntimeError(Messages.ADD_RUNNING_GLOBAL_SEARCH)

        if not mdh_instance_is_running(self.instance.global_search, True):
            raise RuntimeError(
                Messages.ADD_RUNNING_GLOBAL_SEARCH_BY_NAME.format(
                    global_search=self.instance.global_search
                )
            )

        cores = split_global_search_cores(self.instance.selected_cores)
        running_cores = [
            core.name
            for core in
            mdh.core.main.get(only_running=True)
        ]
        for core in cores:
            if core not in running_cores:
                raise RuntimeError(
                    Messages.ADD_RUNNING_CORE_BY_NAME.format(core=core)
                )

        return MdHInstanceConnectionPortObject(
            MdHInstanceConnectionPortObjectSpec(INSTANCE_CONNECTION_TYPE.id),
            {
                FlowVariables.INSTANCE: self.instance.global_search,
                FlowVariables.SELECTED_CORES: cores,
                FlowVariables.IGNORE_ERRORS: self.error_behavior.ignore_errors,
                FlowVariables.IGNORE_FAILED_CONS: self.error_behavior.ignore_failed_connections
            }
        )
