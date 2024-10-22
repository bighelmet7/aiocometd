import asyncio
import reprlib
import logging
from unittest import mock
from enum import Enum, unique

import aiohttp
import pytest

from aiocometd.client import Client
from aiocometd.exceptions import (
    ServerError,
    ClientInvalidOperation,
    TransportError,
    TransportTimeoutError,
    ClientError,
)
from aiocometd.constants import (
    DEFAULT_CONNECTION_TYPE,
    ConnectionType,
    MetaChannel,
    SERVICE_CHANNEL_PREFIX,
    TransportState,
)


@unique
class MockConnectionType(Enum):
    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"
    TYPE4 = "type4"


@pytest.fixture
def cometd_client():
    cometd_url = "http://cometd.local.test"
    return Client(url=cometd_url)


@pytest.fixture
def long_task():
    def helper(result, timeout=None):
        if not isinstance(result, Exception):
            return result

        raise result

    return helper


@pytest.fixture
def mock_transport():
    transport = mock.MagicMock()
    transport.connection_type = DEFAULT_CONNECTION_TYPE

    connect_result = object()
    transport.connect = mock.AsyncMock(return_value=connect_result)

    response = {
        "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value],
        "successful": True,
    }
    transport.handshake = mock.AsyncMock(return_value=response)
    transport.close = mock.AsyncMock()
    return transport


@pytest.fixture
def mock_create_transport(mock_transport):
    with mock.patch(
        "aiocometd.client.create_transport", return_value=mock_transport
    ) as mock_function:
        yield mock_function


def test_init_with_no_connection_types(cometd_client):
    expected = [ConnectionType.WEBSOCKET, ConnectionType.LONG_POLLING]
    assert cometd_client._connection_types == expected


def test_init_with_connection_types_list():
    connection_types = [ConnectionType.LONG_POLLING, ConnectionType.WEBSOCKET]
    client = Client(url="", connection_types=connection_types)

    assert client._connection_types == connection_types


def test_init_with_connection_type_value():
    # NOTE: this tests the _connection_type as a singular value
    # instead of a list
    connection_type = ConnectionType.LONG_POLLING
    client = Client(url="", connection_types=connection_type)

    assert client._connection_types == [connection_type]


def test_subscriptions(cometd_client):
    # NOTE: checks if the aiocometd.Client() sets the subscriptions property
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.subscriptions = {"channel1", "channel2"}

    result = cometd_client.subscriptions
    assert result == cometd_client._transport.subscriptions


def test_subscriptions_emtpy_on_none_transport(cometd_client):
    cometd_client._transport = None
    result = cometd_client.subscriptions

    assert result == set()


def test_connection_type(cometd_client):
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.connection_type = object()

    result = cometd_client.connection_type

    assert result is cometd_client._transport.connection_type


def test_closed_read_only(cometd_client):
    with pytest.raises(AttributeError):
        cometd_client.closed = False


async def test_get_http_session(cometd_client):
    cometd_client._http_session = object()

    session = await cometd_client._get_http_session()
    assert session == cometd_client._http_session


async def test_close_http_session(cometd_client):
    with mock.patch("aiocometd.client.asyncio.sleep") as mock_sleep:
        cometd_client._http_session = mock.MagicMock()
        cometd_client._http_session.closed = False
        cometd_client._http_session.close = mock.AsyncMock()

        await cometd_client._close_http_session()

        cometd_client._http_session.close.assert_called()
        mock_sleep.assert_called_with(
            cometd_client._HTTP_SESSION_CLOSE_TIMEOUT
        )


async def test_close_http_session_already_closed(cometd_client):
    with mock.patch("aiocometd.client.asyncio.sleep") as mock_sleep:
        cometd_client._http_session = mock.MagicMock()
        cometd_client._http_session.closed = True
        cometd_client._http_session.close = mock.AsyncMock()

        await cometd_client._close_http_session()

        cometd_client._http_session.close.assert_not_called()
        mock_sleep.assert_not_called()


def test_pick_connection_type(cometd_client):
    with mock.patch("aiocometd.client.ConnectionType", new=MockConnectionType):
        cometd_client._connection_types = [
            MockConnectionType.TYPE1,
            MockConnectionType.TYPE2,
            MockConnectionType.TYPE3,
        ]
        supported_types = [
            MockConnectionType.TYPE2.value,
            MockConnectionType.TYPE3.value,
            MockConnectionType.TYPE4.value,
        ]

        result = cometd_client._pick_connection_type(supported_types)

        assert result == MockConnectionType.TYPE2


def test_pick_connection_type_without_overlap(cometd_client):
    with mock.patch("aiocometd.client.ConnectionType", new=MockConnectionType):
        cometd_client._connection_types = [
            MockConnectionType.TYPE1,
            MockConnectionType.TYPE2,
        ]
        supported_types = [
            MockConnectionType.TYPE3.value,
            MockConnectionType.TYPE4.value,
        ]

        result = cometd_client._pick_connection_type(supported_types)

        assert result is None


async def test_negotiate_transport_default(
    cometd_client, mock_transport, mock_create_transport, caplog
):
    caplog.set_level(logging.INFO)

    # NOTE: setup the _get_http_session and transport class
    response = {
        "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value],
        "successful": True,
    }
    cometd_client._pick_connection_type = mock.MagicMock(
        return_value=DEFAULT_CONNECTION_TYPE
    )
    cometd_client._verify_response = mock.MagicMock()
    cometd_client.extensions = object()
    cometd_client.auth = object()
    http_session = object()

    cometd_client._get_http_session = mock.AsyncMock(return_value=http_session)
    result = await cometd_client._negotiate_transport()

    assert result == mock_transport
    mock_create_transport.assert_called_with(
        DEFAULT_CONNECTION_TYPE,
        url=cometd_client.url,
        incoming_queue=cometd_client._incoming_queue,
        ssl=cometd_client.ssl,
        extensions=cometd_client.extensions,
        auth=cometd_client.auth,
        json_dumps=cometd_client._json_dumps,
        json_loads=cometd_client._json_loads,
        http_session=http_session,
    )
    mock_transport.handshake.assert_awaited_with(
        cometd_client._connection_types
    )
    cometd_client._verify_response.assert_called_with(response)
    cometd_client._pick_connection_type.assert_called_with(
        response["supportedConnectionTypes"]
    )
    log_message = "Connection types supported by the server: {!r}".format(
        response["supportedConnectionTypes"]
    )
    assert log_message in caplog.messages


async def test_negotiate_transport_error(
    cometd_client, mock_transport, mock_create_transport, caplog
):
    caplog.set_level(logging.INFO)
    response = {
        "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value],
        "successful": True,
    }
    mock_transport.connection_type = DEFAULT_CONNECTION_TYPE
    mock_transport.handshake = mock.AsyncMock(return_value=response)
    cometd_client._pick_connection_type = mock.MagicMock(return_value=None)
    cometd_client.extensions = object()
    cometd_client.auth = object()
    http_session = object()
    cometd_client._get_http_session = mock.AsyncMock(return_value=http_session)

    with pytest.raises(
        ClientError,
        match="None of the connection types offered "
        "by the server are supported.",
    ):
        await cometd_client._negotiate_transport()

    mock_create_transport.assert_called_with(
        DEFAULT_CONNECTION_TYPE,
        url=cometd_client.url,
        incoming_queue=cometd_client._incoming_queue,
        ssl=cometd_client.ssl,
        extensions=cometd_client.extensions,
        auth=cometd_client.auth,
        json_dumps=cometd_client._json_dumps,
        json_loads=cometd_client._json_loads,
        http_session=http_session,
    )
    mock_transport.handshake.assert_awaited_with(
        cometd_client._connection_types
    )
    cometd_client._pick_connection_type.assert_called_with(
        response["supportedConnectionTypes"]
    )
    mock_transport.close.assert_awaited()
    log_message = "Connection types supported by the server: {!r}".format(
        response["supportedConnectionTypes"]
    )
    assert log_message in caplog.messages


async def test_negotiate_transport_non_default(
    cometd_client, mock_transport, mock_create_transport, caplog
):
    caplog.set_level(logging.INFO)
    non_default_type = ConnectionType.WEBSOCKET
    cometd_client._connection_types = [non_default_type]
    response = {
        "supportedConnectionTypes": [
            DEFAULT_CONNECTION_TYPE.value,
            non_default_type.value,
        ],
        "successful": True,
    }
    mock_transport_default = mock.MagicMock()
    mock_transport_default.connection_type = DEFAULT_CONNECTION_TYPE
    mock_transport_default.client_id = "client_id"
    mock_transport_default.handshake = mock.AsyncMock(return_value=response)
    mock_transport_default.reconnect_advice = object()
    mock_transport_default.close = mock.AsyncMock()

    mock_transport_non_default = mock.MagicMock()
    mock_transport_non_default.connection_type = non_default_type
    mock_transport_non_default.client_id = None

    mock_create_transport.side_effect = [
        mock_transport_default,
        mock_transport_non_default,
    ]
    cometd_client._pick_connection_type = mock.MagicMock(
        return_value=non_default_type
    )
    cometd_client._verify_response = mock.MagicMock()
    cometd_client.extensions = object()
    cometd_client.auth = object()
    http_session = object()
    cometd_client._get_http_session = mock.AsyncMock(return_value=http_session)

    result = await cometd_client._negotiate_transport()

    assert result == mock_transport_non_default
    mock_create_transport.assert_has_calls(
        [
            mock.call(
                DEFAULT_CONNECTION_TYPE,
                url=cometd_client.url,
                incoming_queue=cometd_client._incoming_queue,
                ssl=cometd_client.ssl,
                extensions=cometd_client.extensions,
                auth=cometd_client.auth,
                json_dumps=cometd_client._json_dumps,
                json_loads=cometd_client._json_loads,
                http_session=http_session,
            ),
            mock.call(
                non_default_type,
                url=cometd_client.url,
                incoming_queue=cometd_client._incoming_queue,
                client_id=mock_transport_default.client_id,
                ssl=cometd_client.ssl,
                extensions=cometd_client.extensions,
                auth=cometd_client.auth,
                json_dumps=cometd_client._json_dumps,
                json_loads=cometd_client._json_loads,
                reconnect_advice=mock_transport_default.reconnect_advice,
                http_session=http_session,
            ),
        ]
    )
    mock_transport_default.handshake.assert_awaited_with(
        cometd_client._connection_types
    )
    cometd_client._verify_response.assert_called_with(response)
    cometd_client._pick_connection_type.assert_called_with(
        response["supportedConnectionTypes"]
    )
    mock_transport_default.close.assert_awaited()
    log_message = "Connection types supported by the server: {!r}".format(
        response["supportedConnectionTypes"]
    )
    assert log_message in caplog.messages


async def test_open(cometd_client, mock_transport, caplog):
    caplog.set_level(logging.INFO)

    mock_transport.connection_type = ConnectionType.LONG_POLLING
    cometd_client._negotiate_transport = mock.AsyncMock(
        return_value=mock_transport
    )
    cometd_client._verify_response = mock.MagicMock()
    cometd_client._closed = True

    await cometd_client.open()

    cometd_client._negotiate_transport.assert_awaited()

    mock_transport.connect.assert_awaited()
    expected_opening_message = (
        "Opening client with connection types {!r} ...".format(
            [t.value for t in cometd_client._connection_types]
        )
    )
    assert expected_opening_message in caplog.messages

    expected_client_message = "Client opened with connection_type {!r}".format(
        cometd_client.connection_type.value
    )
    assert expected_client_message in caplog.messages


async def test_open_if_already_open(cometd_client, mock_transport):
    cometd_client._negotiate_transport = mock.AsyncMock(
        return_value=mock_transport
    )
    cometd_client._verify_response = mock.MagicMock()
    cometd_client._closed = False

    with pytest.raises(ClientInvalidOperation):
        await cometd_client.open()

    cometd_client._negotiate_transport.assert_not_called()
    mock_transport.connect.assert_not_awaited()
    cometd_client._verify_response.assert_not_called()


async def test_close(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    cometd_client._closed = False
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.client_id = "client_id"
    cometd_client._transport.disconnect = mock.AsyncMock()
    cometd_client._transport.close = mock.AsyncMock()
    cometd_client._close_http_session = mock.AsyncMock()

    await cometd_client.close()

    cometd_client._transport.disconnect.assert_awaited()
    cometd_client._transport.close.assert_awaited()
    cometd_client._close_http_session.assert_awaited()
    assert cometd_client.closed == True

    expected_log = [
        "Closing client...",
        "Client closed.",
    ]
    assert expected_log == caplog.messages


async def test_close_with_pending_messages(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    cometd_client._closed = False
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.client_id = "client_id"
    cometd_client._transport.disconnect = mock.AsyncMock()
    cometd_client._transport.close = mock.AsyncMock()
    cometd_client._close_http_session = mock.AsyncMock()
    cometd_client._incoming_queue = asyncio.Queue()
    cometd_client._incoming_queue.put_nowait(object())

    await cometd_client.close()

    cometd_client._transport.disconnect.assert_awaited()
    cometd_client._transport.close.assert_awaited()
    cometd_client._close_http_session.assert_awaited()
    assert cometd_client.closed == True

    expected_log = [
        "Closing client while {} messages are still pending...".format(
            cometd_client.pending_count
        ),
        "Client closed.",
    ]
    assert expected_log == caplog.messages


async def test_close_if_already_closed(cometd_client):
    cometd_client._closed = True
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.client_id = "client_id"
    cometd_client._transport.disconnect = mock.AsyncMock()
    cometd_client._transport.close = mock.AsyncMock()
    cometd_client._close_http_session = mock.AsyncMock()

    await cometd_client.close()

    cometd_client._transport.disconnect.assert_not_called()
    cometd_client._transport.close.assert_not_called()
    cometd_client._close_http_session.assert_not_called()

    assert cometd_client.closed == True


async def test_close_on_transport_error(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    cometd_client._closed = False
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.client_id = "client_id"
    error = TransportError("description")
    cometd_client._transport.disconnect = mock.AsyncMock(side_effect=error)
    cometd_client._transport.close = mock.AsyncMock()
    cometd_client._close_http_session = mock.AsyncMock()

    with pytest.raises(TransportError, match=str(error)):
        await cometd_client.close()

    assert cometd_client.closed == True
    cometd_client._transport.disconnect.assert_awaited()
    cometd_client._transport.close.assert_not_awaited()
    cometd_client._close_http_session.assert_not_awaited()

    expected_log = [
        "Closing client...",
        "Client closed.",
    ]
    assert expected_log == caplog.messages


async def test_close_no_transport(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    cometd_client._closed = False
    cometd_client._transport = None
    cometd_client._close_http_session = mock.AsyncMock()

    await cometd_client.close()
    assert cometd_client.closed == True
    cometd_client._close_http_session.assert_awaited()

    expected_log = [
        "Closing client...",
        "Client closed.",
    ]
    assert expected_log == caplog.messages


async def test_subscribe(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    response = {
        "channel": MetaChannel.SUBSCRIBE,
        "successful": True,
        "subscription": "channel1",
        "id": "1",
    }
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.subscribe = mock.AsyncMock(return_value=response)
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False

    await cometd_client.subscribe("channel1")

    cometd_client._transport.subscribe.assert_awaited_with("channel1")
    expected_logs = "Subscribed to channel channel1"
    assert expected_logs in caplog.messages

    cometd_client._check_server_disconnected.assert_awaited()


async def test_subscribe_on_closed(cometd_client):
    cometd_client._closed = True
    cometd_client._check_server_disconnected = mock.AsyncMock()

    with pytest.raises(
        ClientInvalidOperation,
        match="Can't send subscribe request while, the client is closed.",
    ):
        await cometd_client.subscribe("channel1")

    cometd_client._check_server_disconnected.assert_not_awaited()


async def test_subscribe_error(cometd_client):
    response = {
        "channel": MetaChannel.SUBSCRIBE,
        "successful": False,
        "subscription": "channel1",
        "id": "1",
    }
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.subscribe = mock.AsyncMock(return_value=response)
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False
    error = ServerError("Subscribe request failed.", response)

    with pytest.raises(ServerError, match=str(error)):
        await cometd_client.subscribe("channel1")

    cometd_client._transport.subscribe.assert_awaited_with("channel1")
    cometd_client._check_server_disconnected.assert_awaited()


async def test_unsubscribe(cometd_client, caplog):
    caplog.set_level(logging.INFO)

    response = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "successful": True,
        "subscription": "channel1",
        "id": "1",
    }
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.unsubscribe = mock.AsyncMock(
        return_value=response
    )
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False

    await cometd_client.unsubscribe("channel1")

    cometd_client._transport.unsubscribe.assert_awaited_with("channel1")
    expected_logs = "Unsubscribed from channel {}".format("channel1")

    assert expected_logs in caplog.messages
    cometd_client._check_server_disconnected.assert_awaited()


async def test_unsubscribe_on_closed(cometd_client):
    cometd_client._closed = True
    cometd_client._check_server_disconnected = mock.AsyncMock()

    with pytest.raises(
        ClientInvalidOperation,
        match="Can't send unsubscribe request while, the client is closed.",
    ):
        await cometd_client.unsubscribe("channel1")

    cometd_client._check_server_disconnected.assert_not_awaited()


async def test_unsubscribe_error(cometd_client):
    response = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "successful": False,
        "subscription": "channel1",
        "id": "1",
    }
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.unsubscribe = mock.AsyncMock(
        return_value=response
    )
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False
    error = ServerError("Unsubscribe request failed.", response)

    with pytest.raises(ServerError, match=str(error)):
        await cometd_client.unsubscribe("channel1")

    cometd_client._transport.unsubscribe.assert_awaited_with("channel1")
    cometd_client._check_server_disconnected.assert_awaited()


async def test_publish(cometd_client):
    response = {"channel": "/channel1", "successful": True, "id": "1"}
    data = {}
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.publish = mock.AsyncMock(return_value=response)
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False

    result = await cometd_client.publish("channel1", data)

    assert result == response
    cometd_client._transport.publish.assert_awaited_with("channel1", data)
    cometd_client._check_server_disconnected.assert_awaited()


async def test_publish_on_closed(cometd_client):
    cometd_client._closed = True
    cometd_client._check_server_disconnected = mock.AsyncMock()

    with pytest.raises(
        ClientInvalidOperation,
        match="Can't publish data while, the client is closed.",
    ):
        await cometd_client.publish("channel1", {})

    cometd_client._check_server_disconnected.assert_not_awaited()


async def test_publish_error(cometd_client):
    response = {"channel": "/channel1", "successful": False, "id": "1"}
    data = {}
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.publish = mock.AsyncMock(return_value=response)
    cometd_client._check_server_disconnected = mock.AsyncMock()
    cometd_client._closed = False
    error = ServerError("Publish request failed.", response)

    with pytest.raises(ServerError, match=str(error)):
        await cometd_client.publish("channel1", data)

    cometd_client._transport.publish.assert_awaited_with("channel1", data)
    cometd_client._check_server_disconnected.assert_awaited()


def test_repr(cometd_client):
    cometd_client.url = "http://example.com"
    expected = (
        "Client({}, {}, connection_timeout={}, ssl={}, "
        "max_pending_count={}, extensions={}, auth={})".format(
            reprlib.repr(cometd_client.url),
            reprlib.repr(cometd_client._connection_types),
            reprlib.repr(cometd_client.connection_timeout),
            reprlib.repr(cometd_client.ssl),
            reprlib.repr(cometd_client._max_pending_count),
            reprlib.repr(cometd_client.extensions),
            reprlib.repr(cometd_client.auth),
        )
    )

    result = repr(cometd_client)

    assert result == expected


def test_verify_response_on_success(cometd_client):
    cometd_client._raise_server_error = mock.MagicMock()
    response = {"channel": "/channel1", "successful": True, "id": "1"}

    cometd_client._verify_response(response)

    cometd_client._raise_server_error.assert_not_called()


def test_verify_response_on_error(cometd_client):
    cometd_client._raise_server_error = mock.MagicMock()
    response = {"channel": "/channel1", "successful": False, "id": "1"}

    cometd_client._verify_response(response)

    cometd_client._raise_server_error.assert_called_with(response)


def test_verify_response_no_successful_status(cometd_client):
    cometd_client._raise_server_error = mock.MagicMock()
    response = {"channel": "/channel1", "id": "1"}

    cometd_client._verify_response(response)

    cometd_client._raise_server_error.assert_not_called()


def test_raise_server_error_meta(cometd_client):
    response = {
        "channel": MetaChannel.SUBSCRIBE,
        "successful": False,
        "id": "1",
    }
    error_message = type(cometd_client)._SERVER_ERROR_MESSAGES[
        response["channel"]
    ]

    with pytest.raises(ServerError, match=error_message):
        cometd_client._raise_server_error(response)


def test_raise_server_error_service(cometd_client):
    response = {
        "channel": SERVICE_CHANNEL_PREFIX + "test",
        "successful": False,
        "id": "1",
    }

    with pytest.raises(ServerError, match="Service request failed."):
        cometd_client._raise_server_error(response)


def test_raise_server_error_publish(cometd_client):
    response = {"channel": "/some/channel", "successful": False, "id": "1"}

    with pytest.raises(ServerError, match="Publish request failed."):
        cometd_client._raise_server_error(response)


async def test_pending_count(cometd_client):
    cometd_client._incoming_queue = asyncio.Queue()
    await cometd_client._incoming_queue.put(1)
    await cometd_client._incoming_queue.put(2)

    assert cometd_client.pending_count == 2


async def test_pending_count_if_none_queue(cometd_client):
    cometd_client._incoming_queue = None

    assert cometd_client.pending_count == 0


async def test_has_pending_messages(cometd_client):
    cometd_client._incoming_queue = asyncio.Queue()
    await cometd_client._incoming_queue.put(1)

    assert cometd_client.has_pending_messages == True


async def test_has_pending_messages_false(cometd_client):
    cometd_client._incoming_queue = None

    assert cometd_client.has_pending_messages == False


async def test_receive_on_closed(cometd_client):
    cometd_client._closed = True
    cometd_client._incoming_queue = None

    with pytest.raises(
        ClientInvalidOperation,
        match="The client is closed and there are no pending messages.",
    ):
        await cometd_client.receive()


async def test_receive_on_closed_and_pending_messages(cometd_client):
    cometd_client._closed = True
    response = {"channel": "/channel1", "data": {}, "id": "1"}
    cometd_client._incoming_queue = mock.MagicMock()
    cometd_client._incoming_queue.qsize.return_value = 1
    cometd_client._get_message = mock.AsyncMock(return_value=response)
    cometd_client._verify_response = mock.MagicMock()

    result = await cometd_client.receive()

    assert result == response
    cometd_client._get_message.assert_awaited_with(
        cometd_client.connection_timeout
    )
    cometd_client._verify_response.assert_called_with(response)


async def test_receive_on_open(cometd_client):
    cometd_client._closed = False
    response = {"channel": "/channel1", "data": {}, "id": "1"}
    cometd_client._incoming_queue = mock.MagicMock()
    cometd_client._incoming_queue.qsize.return_value = 1
    cometd_client._get_message = mock.AsyncMock(return_value=response)
    cometd_client._verify_response = mock.MagicMock()

    result = await cometd_client.receive()

    assert result == response
    cometd_client._get_message.assert_awaited_with(
        cometd_client.connection_timeout
    )
    cometd_client._verify_response.assert_called_with(response)


async def test_receive_on_connection_timeout(cometd_client):
    cometd_client._closed = True
    cometd_client._incoming_queue = mock.MagicMock()
    cometd_client._incoming_queue.qsize.return_value = 1
    cometd_client._get_message = mock.AsyncMock(
        side_effect=TransportTimeoutError()
    )
    cometd_client._verify_response = mock.MagicMock()

    with pytest.raises(TransportTimeoutError):
        await cometd_client.receive()

    cometd_client._get_message.assert_awaited_with(
        cometd_client.connection_timeout
    )
    cometd_client._verify_response.assert_not_called()


async def test_aiter(cometd_client):
    responses = [
        {"channel": "/channel1", "data": {}, "id": "1"},
        {"channel": "/channel2", "data": {}, "id": "2"},
    ]
    cometd_client.receive = mock.AsyncMock(
        side_effect=responses + [ClientInvalidOperation()]
    )

    result = []
    async for message in cometd_client:
        result.append(message)

    assert result == responses


async def test_context_manager(cometd_client):
    cometd_client.open = mock.AsyncMock()
    cometd_client.close = mock.AsyncMock()

    async with cometd_client as client:
        pass

    assert client is cometd_client
    cometd_client.open.assert_awaited()
    cometd_client.close.assert_awaited()


async def test_context_manager_on_enter_error(cometd_client):
    cometd_client.open = mock.AsyncMock(side_effect=TransportError())
    cometd_client.close = mock.AsyncMock()

    with pytest.raises(TransportError):
        async with cometd_client:
            pass

    cometd_client.open.assert_awaited()
    cometd_client.close.assert_awaited()


async def test_get_message_no_timeout(cometd_client, long_task):
    cometd_client._incoming_queue = mock.MagicMock()
    cometd_client._incoming_queue.get = mock.AsyncMock(return_value=object())
    cometd_client._wait_connection_timeout = mock.AsyncMock()
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.wait_for_state = mock.AsyncMock(
        return_value=long_task(None, timeout=1)
    )

    result = await cometd_client._get_message(None)

    assert result is cometd_client._incoming_queue.get.return_value
    cometd_client._wait_connection_timeout.assert_not_awaited()
    cometd_client._transport.wait_for_state.assert_awaited_with(
        TransportState.SERVER_DISCONNECTED
    )


async def test_get_message_with_timeout_not_triggered(
    cometd_client, long_task
):
    cometd_client._incoming_queue = mock.MagicMock()
    get_result = object()
    cometd_client._incoming_queue.get = mock.AsyncMock(
        return_value=long_task(get_result, timeout=None)
    )
    cometd_client._wait_connection_timeout = mock.AsyncMock(
        return_value=long_task(None, timeout=1)
    )
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.wait_for_state = mock.AsyncMock(
        return_value=long_task(None, timeout=1)
    )
    timeout = 2

    result = await cometd_client._get_message(timeout)

    assert result is get_result
    cometd_client._wait_connection_timeout.assert_awaited_with(timeout)
    cometd_client._transport.wait_for_state.assert_awaited_with(
        TransportState.SERVER_DISCONNECTED
    )


async def test_check_server_disconnected_on_disconnected(cometd_client):
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.state = TransportState.SERVER_DISCONNECTED
    cometd_client._transport.last_connect_result = object()

    with pytest.raises(ServerError, match="Connection closed by the server"):
        await cometd_client._check_server_disconnected()


async def test_check_server_disconnected_on_not_disconnected(cometd_client):
    cometd_client._transport = mock.MagicMock()
    cometd_client._transport.state = TransportState.CONNECTED

    await cometd_client._check_server_disconnected()


async def test_check_server_disconnected_on_none_transport(cometd_client):
    cometd_client._transport = None

    await cometd_client._check_server_disconnected()
