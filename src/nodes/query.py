"""Query nodes."""

# Python imports
import itertools
import json
import logging
from pathlib import Path

# 3rd party imports
import knime.extension as knext
import pandas as pd

# Local imports
import mdh
from mdh.types.query import (
    DownloadFormat,
    QueryFilter,
    QueryOutput,
    QueryParameters,
    StreamedOutput
)
from ports.instance_connection import (
    INSTANCE_CONNECTION_TYPE,
    MdHInstanceConnectionPortObject,
    MdHInstanceConnectionPortObjectSpec
)
from ports.metadata_query import (
    METADATA_QUERY_TYPE,
    MdHMetadataQueryPortObject,
    MdHMetadataQueryPortObjectSpec
)
from utils.mdh import (  # noqa[I100,I201]
    get_global_search_headers,
    mdh_download_format_exists,
    mdh_instance_is_global_search,
    mdh_instance_is_running
)
from utils.message import Messages
from utils.parameter import FlowVariables


LOGGER = logging.getLogger(__name__)


__category = knext.category(
    path='/community/mdh',
    level_id='query',
    name='Query',
    description='MdH Query Nodes.',
    icon='icons/mdh.png',
)

####################
# Parameter Groups #
####################


@knext.parameter_group(label='Output configuration')
class Output:  # noqa[D101]

    download_format = knext.StringParameter(
        'Download format',
        'Choose a format for the file that will be created (downloaded).',
        default_value=DownloadFormat.JSON.value,
        enum=[e.value for e in DownloadFormat]
    )
    output_result_file = knext.StringParameter(
        'Output file',
        'The absolute path to a file into which the result is to be streamed.\
        Warning: Existing file will be overwritten.'
    )


########################
# Metadata Query Nodes #
########################


def create_query_parameter(query_config: dict) -> QueryParameters:
    """Get query parameter from query creator dict."""
    filter_functions = []
    for filter in query_config['filters']:
        value_type = 'STR'
        if filter['value_type'] == 'DATE':
            value_type = 'TS'
        if filter['value_type'] == 'NUMBER':
            value_type = 'NUM'

        filter_functions.append(QueryFilter(
            filter['tag'],
            filter['operation'],
            filter['target'],
            value_type
        ))

    query_parameter = QueryParameters(
        filter_functions=filter_functions,
        filter_logic=query_config['filter_logic'],
        selected_tags=query_config['selected_tags'],
        limit=query_config['limit'],
        offset=query_config['offset']
    )
    return query_parameter


@knext.node(
    name='MdH Execute Query to File',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/write_query.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.input_port(
    name='Input port - query',
    description='Query data for this node',
    port_type=METADATA_QUERY_TYPE
)
class MdHExecuteQueryToFileNode(knext.PythonNode):
    """Run a generic query on a MdH Core or Global Search and stream the result into a file.

    Build and run a generic **MdH Search** query via the **Metadata Query Creator** node
    and stream harvested metadata into a file for later retrieval.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    output = Output()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: MdHInstanceConnectionPortObjectSpec,
        __: MdHMetadataQueryPortObjectSpec
    ):
        """Node configuration."""
        if not mdh_download_format_exists(self.output.download_format):
            LOGGER.warning(f' {Messages.QUERY_VALID_DOWNLOAD_FORMAT}')
            config_context.set_warning(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        elif not Path(self.output.output_result_file).is_absolute():
            LOGGER.warning(f' {Messages.QUERY_VALID_OUTPUT_FILE}')
            config_context.set_warning(Messages.QUERY_VALID_OUTPUT_FILE)

        return None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        mdh_connection: MdHInstanceConnectionPortObject,
        mdh_query: MdHMetadataQueryPortObject
    ):
        """Node execution."""
        instance = mdh_connection.data[FlowVariables.INSTANCE]
        query_config = mdh_query.data[FlowVariables.QUERY]

        is_global_search = mdh_instance_is_global_search(instance)
        if not mdh_instance_is_running(instance, is_global_search):
            raise RuntimeError(
                Messages.ADD_RUNNING_INSTANCE_BY_NAME.format(instance=instance)
            )
        if not mdh_download_format_exists(self.output.download_format):
            raise RuntimeError(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        if not Path(self.output.output_result_file).is_absolute():
            raise RuntimeError(Messages.QUERY_VALID_OUTPUT_FILE)

        query_parameter = create_query_parameter(query_config)
        query_output = QueryOutput(
            getattr(DownloadFormat, self.output.download_format.upper()),
            StreamedOutput(
                False,
                Path(self.output.output_result_file)
            )
        )

        if is_global_search:
            mdh.global_search.query.query_via_custom_filters(
                instance,
                query_parameter,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            mdh.core.query.query_via_custom_filters(
                instance,
                query_parameter,
                output=query_output
            )

        return None


@knext.node(
    name='MdH Execute Query to String',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/read_query.png',
    category=__category
)
@knext.input_port(
    name='Input port - connection',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.input_port(
    name='Input port - query',
    description='Query data for this node',
    port_type=METADATA_QUERY_TYPE
)
@knext.output_table(
    'metadata table',
    'A KNIME table with one column containing the queried metadata as a JSON string.'
)
class MdHExecuteQueryToStringNode(knext.PythonNode):
    """Run a generic query on a MdH Core or Global Search and retrieve the result into a KNIME table.

    Build and run a generic **MdH Search** query via the **Metadata Query Creator** node
    and examine harvested metadata directly in a KNIME data table.

    Warning: This node should be used with caution
    when retrieving large amounts of metadata as the available RAM could be exhausted.
    If memory is an issue, use the **Metadata Query to File** node instead.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: MdHInstanceConnectionPortObjectSpec,
        __: MdHMetadataQueryPortObjectSpec
    ):
        """Node configuration."""
        return None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        mdh_connection: MdHInstanceConnectionPortObject,
        mdh_query: MdHMetadataQueryPortObject
    ):
        """Node execution."""
        instance = mdh_connection.data[FlowVariables.INSTANCE]
        query_config = mdh_query.data[FlowVariables.QUERY]

        is_global_search = mdh_instance_is_global_search(instance)
        if not mdh_instance_is_running(instance, is_global_search):
            raise RuntimeError(
                Messages.ADD_RUNNING_INSTANCE_BY_NAME.format(instance=instance)
            )

        if query_config['selected_tags'] and 'SourceFile' not in query_config['selected_tags']:
            query_config['selected_tags'].append('SourceFile')

        query_parameter = create_query_parameter(query_config)
        query_output = QueryOutput()

        if is_global_search:
            result = mdh.global_search.query.query_via_custom_filters(
                instance,
                query_parameter,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            result = mdh.core.query.query_via_custom_filters(
                instance,
                query_parameter,
                output=query_output
            )

        instances = []
        source_files = []
        tags = []
        values = []
        for file in json.loads(result)['data']['mdhSearch']['files']:
            instance = file['instanceName']
            metadata = file['metadata']
            source_file = next(
                itertools.dropwhile(
                    lambda entry: entry['name'] != 'SourceFile',
                    metadata
                )
            )['value']

            for entry in metadata:
                instances.append(instance)
                source_files.append(source_file)
                tags.append(entry['name'])
                values.append(entry['value'])

        table = {
            'Instance': instances,
            'SourceFile': source_files,
            'Tag': tags,
            'Value': values
        }
        return knext.Table.from_pandas(
            pd.DataFrame(table)
        )
