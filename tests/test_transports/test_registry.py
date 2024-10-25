from unittest import mock

import pytest

from aiocometd.transports.registry import (
    create_transport,
    register_transport,
    TRANSPORT_CLASSES,
)
from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportInvalidOperation


@pytest.fixture
def transport_classes():
    yield TRANSPORT_CLASSES

    TRANSPORT_CLASSES.clear()


def test_register_transport(transport_classes):
    connection_type = ConnectionType.LONG_POLLING

    @register_transport(connection_type)
    class FakeTransport:
        "FakeTransport"

    obj = FakeTransport()

    assert obj.connection_type == connection_type
    assert transport_classes[connection_type] == FakeTransport


def test_create_transport(transport_classes):
    transport = object()
    transport_cls = mock.MagicMock(return_value=transport)
    transport_classes[ConnectionType.LONG_POLLING] = transport_cls

    result = create_transport(
        ConnectionType.LONG_POLLING, "arg", kwarg="value"
    )

    assert result == transport
    transport_cls.assert_called_with("arg", kwarg="value")


def test_create_transport_error():
    connection_type = None

    with pytest.raises(
        TransportInvalidOperation,
        match="There is no transport for connection type {!r}".format(
            connection_type
        ),
    ):
        create_transport(connection_type)
