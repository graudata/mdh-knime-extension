"""Query nodes."""

# Python imports
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
    QueryOutput,
    StreamedOutput
)
from ports.instance_connection import (
    INSTANCE_CONNECTION_TYPE,
    MdHInstanceConnectionPortObject,
)
from utils.mdh import (  # noqa[I100,I201]
    get_global_search_headers,
    mdh_download_format_exists,
    mdh_instance_is_global_search,
    mdh_instance_is_running
)
from utils.message import Messages
from utils.parameter import FlowVariables
from utils.paths import is_absolute_file_path


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


@knext.parameter_group(label='Parameter')
class MetadataQueryParameter:  # noqa[D101]

    input_query_file = knext.StringParameter(
        'Path to a GraphQL query file',
        'The absolute path to a file containing valid GraphQL for a MdH search.'
    )
    only_count = knext.BoolParameter(
        'Only count?',
        'Mark as checked if only the number (count) of matching files should be returned.',
        default_value=False
    )


@knext.parameter_group(label='Parameter')
class FileMetadataParameter:  # noqa[D101]

    input_metadata_file = knext.StringParameter(
        'Path to a harvested file',
        'The absolute path to a harvested file to query metadata.'
    )
    only_count = knext.BoolParameter(
        'Only count?',
        'Mark as checked if only the number (count) of matching files should be returned.',
        default_value=False
    )


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


@knext.node(
    name='Metadata Query to File',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/write_query.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
class MetadataQueryToFileNode(knext.PythonNode):
    """Run a generic GraphQL query on a MdH Core or Global Search and stream the result into a file.

    By specifying a file containing a generic **GraphQL MdH Search**,
    the harvested metadata can be streamed into a file for later retrieval.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    parameter = MetadataQueryParameter()
    output = Output()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
    ):
        """Node configuration."""
        if not is_absolute_file_path(Path(self.parameter.input_query_file)):
            LOGGER.warning(f' {Messages.EXISTING_GRAPHQL_FILE}')
            config_context.set_warning(Messages.EXISTING_GRAPHQL_FILE)
        elif not mdh_download_format_exists(self.output.download_format):
            LOGGER.warning(f' {Messages.QUERY_VALID_DOWNLOAD_FORMAT}')
            config_context.set_warning(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        elif not Path(self.output.output_result_file).is_absolute():
            LOGGER.warning(f' {Messages.QUERY_VALID_OUTPUT_FILE}')
            config_context.set_warning(Messages.QUERY_VALID_OUTPUT_FILE)

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
        if not is_absolute_file_path(Path(self.parameter.input_query_file)):
            raise RuntimeError(Messages.EXISTING_GRAPHQL_FILE)
        if not mdh_download_format_exists(self.output.download_format):
            raise RuntimeError(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        if not Path(self.output.output_result_file).is_absolute():
            raise RuntimeError(Messages.QUERY_VALID_OUTPUT_FILE)

        query_output = QueryOutput(
            getattr(DownloadFormat, self.output.download_format.upper()),
            StreamedOutput(
                False,
                Path(self.output.output_result_file)
            ),
            self.parameter.only_count
        )
        if is_global_search:
            mdh.global_search.query.query_via_file(
                instance,
                self.parameter.input_query_file,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            mdh.core.query.query_via_file(
                instance,
                self.parameter.input_query_file,
                output=query_output
            )

        return None


@knext.node(
    name='Metadata Query to String',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/read_query.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.output_table(
    'metadata table',
    'A KNIME table with one column containing the queried metadata as a JSON string.'
)
class MetadataQueryToStringNode(knext.PythonNode):
    """Run a generic GraphQL query on a MdH Core or Global Search and retrieve the result into a KNIME table.

    By specifying a file containing a generic **GraphQL MdH Search**,
    the harvested metadata can be examined directly in a KNIME data table.

    Warning: This node should be used with caution
    when dealing with large amounts of metadata, as the available RAM could be exceeded.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    parameter = MetadataQueryParameter()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
    ):
        """Node configuration."""
        if not is_absolute_file_path(Path(self.parameter.input_query_file)):
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
        if not mdh_instance_is_running(instance, is_global_search):
            raise RuntimeError(
                Messages.ADD_RUNNING_INSTANCE_BY_NAME.format(instance=instance)
            )
        if not is_absolute_file_path(Path(self.parameter.input_query_file)):
            raise RuntimeError(Messages.EXISTING_GRAPHQL_FILE)

        query_output = QueryOutput(
            only_count=self.parameter.only_count
        )

        if is_global_search:
            result = mdh.global_search.query.query_via_file(
                instance,
                self.parameter.input_query_file,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            result = mdh.core.query.query_via_file(
                instance,
                self.parameter.input_query_file,
                output=query_output
            )

        metadata = json.dumps(json.loads(result)['data']['mdhSearch'])
        df = pd.DataFrame(
            {
                'metadata': metadata
            },
            index=[0],
        )
        return knext.Table.from_pandas(df)


#######################
# File Metadata Nodes #
#######################


@knext.node(
    name='File Metadata to File',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/write_file.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
class FileMetadataToFileNode(knext.PythonNode):
    """Query metadata of a harvested file on a MdH Core or Global Search and stream the result into a file.

    By specifying a file, which has already been harvested,
    the metadata of that file can be streamed into a file for later retrieval.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    parameter = FileMetadataParameter()
    output = Output()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
    ):
        """Node configuration."""
        if not Path(self.parameter.input_metadata_file).is_absolute():
            config_context.set_warning(Messages.QUERY_VALID_INPUT_FILE)
        elif not mdh_download_format_exists(self.output.download_format):
            config_context.set_warning(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        elif not Path(self.output.output_result_file).is_absolute():
            config_context.set_warning(Messages.QUERY_VALID_OUTPUT_FILE)

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
        if not Path(self.parameter.input_metadata_file).is_absolute():
            raise RuntimeError(Messages.QUERY_VALID_INPUT_FILE)
        if not mdh_download_format_exists(self.output.download_format):
            raise RuntimeError(Messages.QUERY_VALID_DOWNLOAD_FORMAT)
        if not Path(self.output.output_result_file).is_absolute():
            raise RuntimeError(Messages.QUERY_VALID_OUTPUT_FILE)

        query_output = QueryOutput(
            getattr(DownloadFormat, self.output.download_format.upper()),
            StreamedOutput(
                False,
                Path(self.output.output_result_file)
            ),
            self.parameter.only_count
        )
        if is_global_search:
            mdh.global_search.query.filepath(
                instance,
                self.parameter.input_metadata_file,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            mdh.core.query.filepath(
                instance,
                self.parameter.input_metadata_file,
                output=query_output
            )
        return None


@knext.node(
    name='File Metadata to String',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/read_file.png',
    category=__category
)
@knext.input_port(
    name='Input port',
    description='Connection data for this node',
    port_type=INSTANCE_CONNECTION_TYPE
)
@knext.output_table(
    'metadata table',
    'A KNIME table with one column containing the queried metadata as a JSON string.'
)
class FileMetadataToStringNode(knext.PythonNode):
    """Query metadata of a harvested file on a MdH Core or Global Search and retrieve the result into a KNIME table.

    By specifying a file, which has already been harvested,
    the metadata of that file can be examined directly in a KNIME data table.

    Warning: This node should be used with caution
    when dealing with large amounts of metadata, as the available RAM could be exceeded.

    For any questions, refer to the [API-Documentation](https://metadatahub.de/documentation/3.0/graphql/)
    (mdhSearch) or send an e-mail to mdh-support@graudata.com.
    We are pleased to help.
    """

    parameter = FileMetadataParameter()

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        _: knext.BinaryPortObjectSpec
    ):
        """Node configuration."""
        if not Path(self.parameter.input_metadata_file).is_absolute():
            config_context.set_warning(Messages.QUERY_VALID_INPUT_FILE)

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
        if not Path(self.parameter.input_metadata_file).is_absolute():
            raise RuntimeError(Messages.QUERY_VALID_INPUT_FILE)

        query_output = QueryOutput(
            only_count=self.parameter.only_count
        )

        if is_global_search:
            result = mdh.global_search.query.filepath(
                instance,
                self.parameter.input_metadata_file,
                output=query_output,
                global_search_headers=get_global_search_headers(mdh_connection.data)
            )
        else:
            result = mdh.core.query.filepath(
                instance,
                self.parameter.input_metadata_file,
                output=query_output
            )

        metadata = json.dumps(json.loads(result)['data']['mdhSearch'])
        df = pd.DataFrame(
            {
                'metadata': metadata
            },
            index=[0],
        )
        return knext.Table.from_pandas(df)
