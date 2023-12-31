"""Custom connection port."""

# Python imports
import pickle

# 3rd party imports
import knime.extension as knext


class MdHInstanceConnectionPortObjectSpec(knext.BinaryPortObjectSpec):
    """Spec for connecting two MdH nodes."""

    pass


class MdHInstanceConnectionPortObject(knext.PortObject):
    """Port for connecting two MdH nodes."""

    def __init__(
        self,
        spec: MdHInstanceConnectionPortObjectSpec,
        connection_data: dict
    ) -> None:
        """Port for connecting two MdH nodes."""
        super().__init__(spec)
        self._data = connection_data

    def serialize(self) -> bytes:
        """Serializes the object to bytes."""
        return pickle.dumps(self._data)

    @classmethod
    def deserialize(
        cls,
        spec: MdHInstanceConnectionPortObjectSpec,
        data: bytes
    ) -> 'MdHInstanceConnectionPortObject':
        """Creates the port object from its spec and storage."""
        return cls(spec, pickle.loads(data))

    @property
    def data(self) -> dict:
        """Data property."""
        return self._data


INSTANCE_CONNECTION_TYPE = knext.port_type(
    name='PortType.MdHInstanceConnection',
    object_class=MdHInstanceConnectionPortObject,
    spec_class=MdHInstanceConnectionPortObjectSpec
)
