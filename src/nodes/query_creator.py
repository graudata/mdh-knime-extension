"""Query Creator node."""

# Python imports
import logging
from dataclasses import dataclass

# 3rd party imports
import knime.extension as knext

# Local imports
from .query import __category

LOGGER = logging.getLogger(__name__)


class IsNotEmpty(knext.Condition):

    def __init__(self, subject) -> None:
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
    TAG_EXISTS = \
        ('tag exists', 'check if the metadata tag exists')
    TAG_NOT_EXISTS = \
        ('tag does not exist', 'check if the metadata tag does NOT exist')
    VALUE_EMPTY = \
        ('value is empty', 'check if the metadata value is empty')
    VALUE_NOT_EMPTY = \
        ('value is not empty', 'check if the metadata value is NOT empty')
    VALUE_CONTAINS = \
        ('contains', 'check if the metadata value contains provided string')
    VALUE_NOT_CONTAINS = \
        ('not contains', 'check if the metadata value does not contain provided string')
    VALUE_IS_EQUAL = \
        ('is equal', 'check if the metadata value is equal')
    VALUE_IS_NOT_EQUAL = \
        ('is not equal', 'check if the metadata value is not equal')
    VALUE_IS_GREATER = \
        ('is greater', 'check if the metadata value is greater')
    VALUE_IS_SMALLER = \
        ('is smaller', 'check if the metadata value is smaller')


class TagTypeOptions(knext.EnumParameterOptions):
    STRING = \
        ('string', 'TODO')
    NUMBER = \
        ('number', 'TODO')
    DATE = \
        ('date', 'TODO')


class StringOperationOptions(knext.EnumParameterOptions):
    TAG_EXISTS = OperationOptions.TAG_EXISTS
    TAG_NOT_EXISTS = OperationOptions.TAG_NOT_EXISTS
    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_CONTAINS = OperationOptions.VALUE_CONTAINS
    VALUE_NOT_CONTAINS = OperationOptions.VALUE_NOT_CONTAINS
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL


class NumberOperationOptions(knext.EnumParameterOptions):
    TAG_EXISTS = OperationOptions.TAG_EXISTS
    TAG_NOT_EXISTS = OperationOptions.TAG_NOT_EXISTS
    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL
    VALUE_IS_GREATER = OperationOptions.VALUE_IS_GREATER
    VALUE_IS_SMALLER = OperationOptions.VALUE_IS_SMALLER


class DateOperationOptions(knext.EnumParameterOptions):
    VALUE_EMPTY = OperationOptions.VALUE_EMPTY
    VALUE_NOT_EMPTY = OperationOptions.VALUE_NOT_EMPTY
    VALUE_IS_EQUAL = OperationOptions.VALUE_IS_EQUAL
    VALUE_IS_NOT_EQUAL = OperationOptions.VALUE_IS_NOT_EQUAL
    VALUE_IS_GREATER = OperationOptions.VALUE_IS_GREATER
    VALUE_IS_SMALLER = OperationOptions.VALUE_IS_SMALLER


class StringTemplate:
    pass

class NumberTemplate:
    pass

class DateTemplate:
    pass

class FilterTemplate:
    pass


def setup_string_cls(cls):
    operation = knext.EnumParameter(
        'Operation',
        'TODO',
        StringOperationOptions.VALUE_CONTAINS.name,
        StringOperationOptions
    )
    value = knext.StringParameter(
        'Value',
        'TODO'
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

    setattr(cls, 'operation', operation)
    setattr(cls, 'value', value)

def setup_number_cls(cls):
    operation = knext.EnumParameter(
        'Operation',
        'TODO',
        NumberOperationOptions.VALUE_IS_EQUAL.name,
        NumberOperationOptions
    )
    value = knext.StringParameter(
        'Value',
        'TODO'
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

    setattr(cls, 'operation', operation)
    setattr(cls, 'value', value)

def setup_date_cls(cls):
    operation = knext.EnumParameter(
        'Operation',
        'TODO',
        DateOperationOptions.VALUE_IS_EQUAL.name,
        DateOperationOptions
    )
    value = knext.DateTimeParameter(
        'Value',
        'TODO'
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

    setattr(cls, 'operation', operation)
    setattr(cls, 'value', value)


def setup_filter_cls(cls, i):
    tag = knext.StringParameter(
        'Tag',
        'TODO'
    )
    value_type = knext.EnumParameter(
        'Value Type',
        'TODO',
        TagTypeOptions.STRING.name,
        TagTypeOptions
    )

    date_type = type(f'Date{i}', (DateTemplate, ), {})
    setup_date_cls(date_type)
    decorator_cls = knext.parameter_group(label='')
    date = decorator_cls(date_type)()
    date.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.DATE.name]
        ),
        knext.Effect.SHOW
    )

    number_type = type(f'Number{i}', (NumberTemplate, ), {})
    setup_number_cls(number_type)
    decorator_cls = knext.parameter_group(label='')
    number = decorator_cls(number_type)()
    number.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.NUMBER.name]
        ),
        knext.Effect.SHOW
    )

    string_type = type(f'String{i}', (StringTemplate, ), {})
    setup_string_cls(string_type)
    decorator_cls = knext.parameter_group(label='')
    string = decorator_cls(string_type)()
    string.rule(
        knext.OneOf(
            value_type,
            [TagTypeOptions.STRING.name]
        ),
        knext.Effect.SHOW
    )

    setattr(cls, 'tag', tag)
    setattr(cls, 'value_type', value_type)
    setattr(cls, 'date', date)
    setattr(cls, 'number', number)
    setattr(cls, 'string', string)


@knext.node(
    name='Metadata Query Creator',
    node_type=knext.NodeType.SOURCE,
    icon_path='icons/mdh.png',
    category=__category
)
class MetadataQueryCreatorNode(knext.PythonNode):
    """TODO"""
    _MAX_NUM_FILTERS = 100

    filter_logic = knext.StringParameter(
        'Filter Logic',
        'TODO',
        'f1'
    )

    def __init__(self):
        t = type('Filter1', (FilterTemplate, ), {})
        setup_filter_cls(t, 1)
        decorator_cls = knext.parameter_group(label='Filter (f1)')
        setattr(self, 'f1', decorator_cls(t)())
        old_f = getattr(self, 'f1')
        for i in range(2, self._MAX_NUM_FILTERS):
            t = type(f'Filter{i}', (FilterTemplate, ), {})
            setup_filter_cls(t, i)
            decorator_cls = knext.parameter_group(label=f'Filter (f{i})')
            setattr(self, f'f{i}', decorator_cls(t)().rule(IsNotEmpty(old_f), knext.Effect.SHOW))
            old_f = getattr(self, f'f{i}')

    def configure(
        self,
        config_context: knext.ConfigurationContext,
    ):
        """Node configuration."""
        pass

    def execute(
        self,
        exec_context: knext.ExecutionContext,
    ):
        """Node execution."""
        pass
