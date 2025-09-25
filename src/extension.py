"""Root level and node imports."""

# 3rd party imports
import knime.extension as knext


knext.category(
    path='/community',
    level_id='mdh',
    name='Metadata-Hub',
    description='Metadata-Hub.',
    icon='icons/mdh.png',
)

# Node imports
import nodes.config  # noqa[I202,F404]
import nodes.query  # noqa[I202,F404]
import nodes.query_creator  # noqa[I202,F404]
