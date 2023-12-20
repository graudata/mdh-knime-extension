"""Query Creator node."""

# Python imports
import contextlib
import logging
import re
from dataclasses import dataclass
from enum import StrEnum

# 3rd party imports
import knime.extension as knext
from mdh.types.query._parameters._base import DEFAULT_SELECTED_TAGS

# Local imports
from .query import __category
from ports.metadata_query import (
    MdHMetadataQueryPortObject,
    MdHMetadataQueryPortObjectSpec,
    METADATA_QUERY_TYPE
)
from utils.parameter import FlowVariables
from utils.message import Messages


LOGGER = logging.getLogger(__name__)


class TagIsNotEmpty(knext.Condition):
    """A Condition that evaluates to true if the property tag is not empty."""

    def __init__(self, subject) -> None:
        """A Condition that evaluates to true if the property tag is not empty."""
        super().__init__()
        self._subject = subject

    def to_dict(self):
        return {
            'properties': {
                'tag': {
                    'not': {
                        'const': ''
                    }
                }
            }
        }

    @property
    def subjects(self):
        return [self._subject]


@dataclass
class OperationOptions:
    """All available filter operations."""

    TAG_EXISTS = \
        ('exists', 'check if the metadata tag exists')
    TAG_NOT_EXISTS = \
        ('not exists', 'check if the metadata tag does NOT exist')
    VALUE_EMPTY = \
        ('value is empty', 'check if the metadata value is empty')
    VALUE_NOT_EMPTY = \
        ('value is not empty', 'check if the metadata value is NOT empty')
    VALUE_CONTAINS = \
        ('value contains', 'check if the metadata value contains target')
    VALUE_NOT_CONTAINS = \
        ('value not contains', 'check if the metadata value does not contain target')
    VALUE_IS_EQUAL = \
        ('value is equal', 'check if the metadata value is equal to target')
    VALUE_IS_NOT_EQUAL = \
        ('value is not equal', 'check if the metadata value is not equal to target')
    VALUE_IS_GREATER = \
        ('value is greater', 'check if the metadata value is greater than target')
    VALUE_IS_SMALLER = \
        ('value is smaller', 'check if the metadata value is smaller than target')


class TagTypeOptions(knext.EnumParameterOptions):
    """Tag type options."""

    STRING = ('string', 'string related operations')
    NUMBER = ('number', 'number related operations')
    DATE = ('date', 'date related operations')


class KnimeToMdHOperationsMap(StrEnum):
    """Mapping of KNIME operations to MdH operations."""

    TAG_EXISTS = 'EXISTS'
    TAG_NOT_EXISTS = 'NOT_EXISTS'
    VALUE_EMPTY = 'EMPTY'
    VALUE_NOT_EMPTY = 'NOT_EMPTY'
    VALUE_CONTAINS = 'CONTAINS'
    VALUE_NOT_CONTAINS = 'NOT_CONTAINS'
    VALUE_IS_EQUAL = 'EQUAL'
    VALUE_IS_NOT_EQUAL = 'NOT_EQUAL'
    VALUE_IS_GREATER = 'GREATER'
    VALUE_IS_SMALLER = 'SMALLER'


class StringOperationOptions(knext.EnumParameterOptions):
    """Available filter operations for strings."""

    TAG_EXISTS = OperationOptions.TAG_EXISTS
    TAG_NOT_EXISTS = OperationOptions.TAG_NOT_EXISTS
    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_CONTAINS = OperationOptions.VALUE_CONTAINS
    VALUE_NOT_CONTAINS = OperationOptions.VALUE_NOT_CONTAINS
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL


class NumberOperationOptions(knext.EnumParameterOptions):
    """Available filter operations for numbers."""

    TAG_EXISTS = OperationOptions.TAG_EXISTS
    TAG_NOT_EXISTS = OperationOptions.TAG_NOT_EXISTS
    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL
    VALUE_IS_GREATER = OperationOptions.VALUE_IS_GREATER
    VALUE_IS_SMALLER = OperationOptions.VALUE_IS_SMALLER


class DateOperationOptions(knext.EnumParameterOptions):
    """Available filter operations for datetimes."""

    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL
    VALUE_IS_GREATER = OperationOptions.VALUE_IS_GREATER
    VALUE_IS_SMALLER = OperationOptions.VALUE_IS_SMALLER


def inject_string_parameters(string):
    """Inject string parameters."""
    operation = knext.EnumParameter(
        'Operation',
        'Filter operations for strings.',
        StringOperationOptions.VALUE_CONTAINS.name,
        StringOperationOptions
    )
    target = knext.StringParameter(
        'Target',
        'Target string against which the metadata value is to be compared'
    ).rule(
        knext.OneOf(
            operation,
            [
                StringOperationOptions.TAG_EXISTS.name,
                StringOperationOptions.TAG_NOT_EXISTS.name,
                StringOperationOptions.VALUE_EMPTY.name,
                StringOperationOptions.VALUE_NOT_EMPTY.name
            ]
        ),
        knext.Effect.HIDE
    )

    string.operation = operation
    string.target = target


def inject_number_parameters(number):
    """Inject number parameters."""
    operation = knext.EnumParameter(
        'Operation',
        'Filter operations for numbers.',
        NumberOperationOptions.VALUE_IS_EQUAL.name,
        NumberOperationOptions,
    )
    target = knext.StringParameter(
        'Target',
        'Target number against which the metadata value is to be compared'
    ).rule(
        knext.OneOf(
            operation,
            [
                NumberOperationOptions.TAG_EXISTS.name,
                NumberOperationOptions.TAG_NOT_EXISTS.name,
                NumberOperationOptions.VALUE_EMPTY.name,
                NumberOperationOptions.VALUE_NOT_EMPTY.name
            ]
        ),
        knext.Effect.HIDE
    )

    number.operation = operation
    number.target = target


def inject_date_parameters(date):
    """Inject date parameters."""
    operation = knext.EnumParameter(
        'Operation',
        'Filter operations for datetimes.',
        DateOperationOptions.VALUE_IS_EQUAL.name,
        DateOperationOptions
    )
    target = knext.DateTimeParameter(
        'Target',
        'Target date against which the metadata value is to be compared',
        show_time=True,
    ).rule(
        knext.OneOf(
            operation,
            [
                DateOperationOptions.VALUE_EMPTY.name,
                DateOperationOptions.VALUE_NOT_EMPTY.name,
            ]
        ),
        knext.Effect.HIDE
    )

    date.operation = operation
    date.target = target


def inject_filter_parameters(filter, filter_idx):
    """Inject filter parameters."""
    value_type = knext.EnumParameter(
        'Type',
        'Select a type to change the operation options accordingly.',
        TagTypeOptions.STRING.name,
        TagTypeOptions,
        style=knext.EnumParameter.Style.VALUE_SWITCH
    )
    tag = knext.StringParameter(
        'Tag',
        'Choose a metadata tag, '
        'e.g. *SourceFile*, *FileName*, *FileSize*, *FileType*, *FileAccessDate*, ...'
    )

    date_type = type(f'Date{filter_idx}', (), {})
    decorator_cls = knext.parameter_group(label='Date')
    date = decorator_cls(date_type)()
    inject_date_parameters(date)
    date.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.DATE.name]
        ),
        knext.Effect.SHOW
    )

    number_type = type(f'Number{filter_idx}', (), {})
    decorator_cls = knext.parameter_group(label='Number')
    number = decorator_cls(number_type)()
    inject_number_parameters(number)
    number.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.NUMBER.name]
        ),
        knext.Effect.SHOW
    )

    string_type = type(f'String{filter_idx}', (), {})
    decorator_cls = knext.parameter_group(label='String')
    string = decorator_cls(string_type)()
    inject_string_parameters(string)
    string.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.STRING.name]
        ),
        knext.Effect.SHOW
    )

    filter.value_type = value_type
    filter.tag = tag
    filter.date = date
    filter.number = number
    filter.string = string


@knext.parameter_group(label='Filter configuration')
class FilterConfiguration:  # noqa[D101]

    filter_logic = knext.StringParameter(
        'Filter logic',
        'For each appended filter, it is up to you to add the corresponding filter logic. '
        'Allowed logical connectives are conjunction, disjunction and negation. '
        'For example, if you have three filters *f0*, *f1* and *f2* you are able to '
        'create the expression *f0 or (f1 and not f2)*.',
        'f0'
    )
    selected_tags = knext.StringParameter(
        'Selected tags',
        'The selected tags which are part of the query result set.',
        ', '.join(DEFAULT_SELECTED_TAGS)
    )
    limit = knext.IntParameter(
        'Limit',
        'Limit the result set.',
        0,
        min_value=0,
        is_advanced=True
    )
    offset = knext.IntParameter(
        'Offset',
        'Offset the result set',
        0,
        min_value=0,
        is_advanced=True
    )
    only_count = knext.BoolParameter(
        'Only count?',
        'Mark as checked if only the number (count) of matching files should be returned.',
        default_value=False,
        is_advanced=True
    )


@knext.node(
    name='Metadata Query Creator',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/config.png',
    category=__category
)
@knext.output_port(
    name='Output port - query',
    description='Connection data for MdH nodes',
    port_type=METADATA_QUERY_TYPE
)
class MetadataQueryCreatorNode(knext.PythonNode):
    """Create simple or complex metadata queries for downstream query nodes.

    Defaults to a maximum of 20 filters.
    """

    _MAX_NUM_FILTERS = 20
    _RE_FILTER_MATCHES = r'f\d+'

    filter_configuration = FilterConfiguration()

    def __init__(self):
        """Create simple or complex metadata queries for downstream query nodes."""
        filter = self._create_filter_and_inject_parameters(0)
        setattr(self, 'f0', filter)
        last_filter = filter

        for i in range(1, self._MAX_NUM_FILTERS + 1):
            filter = self._create_filter_and_inject_parameters(i)
            filter.rule(TagIsNotEmpty(last_filter), knext.Effect.SHOW)
            setattr(self, f'f{i}', filter)
            last_filter = filter

    def _create_filter_and_inject_parameters(self, filter_idx: int):
        filter_type = type(f'Filter{filter_idx}', (), {})
        decorator_cls = knext.parameter_group(label=f'Filter (f{filter_idx})')
        filter = decorator_cls(filter_type)()
        inject_filter_parameters(filter, filter_idx)
        return filter

    def _get_filters(self):
        vars_ = vars(self)
        filters = []
        for filter_key in vars_:
            if not filter_key.startswith('f'):
                continue

            with contextlib.suppress(ValueError):
                int(filter_key[1:])

            filter = vars_[filter_key]

            if not filter.tag:
                continue

            filters.append((filter_key, filter))

        return filters

    def configure(
        self,
        config_context: knext.ConfigurationContext,
    ):
        """Node configuration."""
        filter_logic = self.filter_configuration.filter_logic
        filter_matches = set(re.findall(self._RE_FILTER_MATCHES, filter_logic))
        for filter_key, filter in self._get_filters():
            if filter_key not in filter_matches:
                config_context.set_warning(
                    Messages.QUERY_VALID_FILTER_LOGIC.format(
                        filter_key=filter_key,
                        filter_logic=filter_logic
                    )
                )
        return MdHMetadataQueryPortObjectSpec(METADATA_QUERY_TYPE.id)

    def execute(
        self,
        exec_context: knext.ExecutionContext,
    ):
        """Node execution."""
        filter_logic = self.filter_configuration.filter_logic
        selected_tags = self.filter_configuration.selected_tags
        limit = self.filter_configuration.limit
        offset = self.filter_configuration.offset
        only_count = self.filter_configuration.only_count

        query = {
            'filter_logic': filter_logic,
            'selected_tags': selected_tags.split(', '),
            'limit': limit if limit != 0 else None,
            'offset': offset,
            'only_count': only_count,
            'filters': []
        }

        filter_matches = set(re.findall(self._RE_FILTER_MATCHES, filter_logic))
        for filter_key, filter in self._get_filters():
            if filter_key not in filter_matches:
                raise RuntimeError(
                    Messages.QUERY_VALID_FILTER_LOGIC.format(
                        filter_key=filter_key,
                        filter_logic=filter_logic
                    )
                )
            operation_group = filter.string
            if filter.value_type == TagTypeOptions.NUMBER.name:
                operation_group = filter.number
            if filter.value_type == TagTypeOptions.DATE.name:
                operation_group = filter.date

            query['filters'].append({
                'tag': filter.tag,
                'value_type': filter.value_type,
                'operation': getattr(KnimeToMdHOperationsMap, operation_group.operation),
                'target': str(operation_group.target)
            })

        return MdHMetadataQueryPortObject(
            MdHMetadataQueryPortObjectSpec(METADATA_QUERY_TYPE.id),
            {
                FlowVariables.QUERY: query
            }
        )
