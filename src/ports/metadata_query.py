"""Custom metadata query port."""

# Python imports
import pickle

# 3rd party imports
import knime.extension as knext


class MdHMetadataQueryPortObject(knext.PortObject):
    """Port for forwarding a metadata query."""

    def __init__(
        self,
        spec: knext.BinaryPortObjectSpec,
        metadata_query: dict
    ) -> None:
        """Port for forwarding a metadata query."""
        super().__init__(spec)
        self._data = metadata_query

    def serialize(self) -> bytes:
        """Serializes the object to bytes."""
        return pickle.dumps(self._data)

    @classmethod
    def deserialize(
        cls,
        spec: knext.BinaryPortObjectSpec,
        data: bytes
    ) -> 'MdHMetadataQueryPortObject':
        """Creates the port object from its spec and storage."""
        return cls(spec, pickle.loads(data))

    @property
    def data(self) -> dict:
        """Data property."""
        return self._data


METADATA_QUERY_TYPE = knext.port_type(
    name='PortType.MdHMetadataQueryPortObject',
    object_class=MdHMetadataQueryPortObject,
    spec_class=knext.BinaryPortObjectSpec
)
