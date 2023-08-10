"""Configuration nodes."""

# Python imports
import logging

# 3rd party imports
import knime.extension as knext

# Local imports
import mdh
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


@knext.parameter_group(label='Error Handling')
class ErrorHandling:  # noqa[D101]

    ignore_errors = knext.BoolParameter(
        'Ignore errors',
        'Ignore errors of single MdH Core instances',
        default_value=False
    )
    ignore_failed_connections = knext.BoolParameter(
        'Ignore failed connections',
        'Ignore failed connections to MdH Core instances',
        default_value=False
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
class CoreConfigurationNode(knext.PythonNode):
    """Select a running MdH Core instance.

    A list of **MdH Core** instances corresponding to your current MdH environment is retrieved.
    The node is used for setting flow variables, which are used by downstreaming MdH nodes.
    """

    instance = CoreInstance()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
        cores = mdh.core.main.get(only_running=True)
        if not cores:
            LOGGER.warning(f' {Messages.ADD_RUNNING_CORE}')
            config_context.set_warning(Messages.ADD_RUNNING_CORE)

        return None

    def execute(self, exec_context: knext.ExecutionContext):
        """Node execution."""
        if not self.instance.core:
            raise RuntimeError(Messages.ADD_RUNNING_CORE)
        if not mdh_instance_is_running(self.instance.core, False):
            raise RuntimeError(
                f'Please add a running MdH Core called \'{self.core}\''
            )

        license_ = mdh.core.licensing.info(self.instance.core)
        if license_['licenseData'] is None:
            exec_context.set_warning(
                f'MdH Core \'{self.instance.core}\' does not have an active license'
            )
        if not license_['validKey']:
            exec_context.set_warning(
                f'MdH Core \'{self.instance.core}\' does not have a valid license'
            )

        exec_context.flow_variables[FlowVariables.INSTANCE] = self.instance.core

        return None


@knext.node(
    name='MdH Global Search Selection',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/config.png',
    category=__category
)
class GlobalSearchConfigurationNode(knext.PythonNode):
    """Select a running MdH Global Search instance.

    A list of **MdH Global Search** instances corresponding
    to your current MdH environment is retrieved.
    The node is used for setting flow variables, which are used by downstreaming MdH nodes.
    """

    instance = GlobalSearchInstance()
    error_behavior = ErrorHandling()

    def configure(self, config_context: knext.ConfigurationContext):
        """Node configuration."""
        global_searches = mdh.global_search.main.get(only_running=True)
        if not global_searches:
            LOGGER.warning(f' {Messages.ADD_RUNNING_GLOBAL_SEARCH}')
            config_context.set_warning(Messages.ADD_RUNNING_GLOBAL_SEARCH)
        return None

    def execute(self, exec_context: knext.ExecutionContext):
        """Node execution."""
        if not self.instance.global_search:
            raise RuntimeError(Messages.ADD_RUNNING_GLOBAL_SEARCH)
        if not mdh_instance_is_running(self.instance.global_search, True):
            raise RuntimeError(
                f'Please add a running MdH Global Search called \'{self.instance.global_search}\''
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
                    f'Unable to match provided name \'{core}\' with any active MdH Core instance.'
                )
            license_ = mdh.core.licensing.info(core)
            if license_['licenseData'] is None:
                raise RuntimeError(
                    f'MdH Core \'{core}\' does not have an active license'
                )
            if not license_['validKey']:
                raise RuntimeError(
                    f'MdH Core \'{core}\' does not have a valid license'
                )
        
        exec_context.flow_variables[FlowVariables.INSTANCE] = self.instance.global_search
        exec_context.flow_variables[FlowVariables.SELECTED_CORES] = self.instance.selected_cores
        exec_context.flow_variables[FlowVariables.IGNORE_ERRORS] = self.error_behavior.ignore_errors
        exec_context.flow_variables[FlowVariables.IGNORE_FAILED_CONS] = \
            self.error_behavior.ignore_failed_connections

        return None
