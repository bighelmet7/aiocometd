import asyncio
import logging
from unittest import mock

import aiohttp
import pytest

from aiocometd.transports.base import TransportBase
from aiocometd.constants import (
    ConnectionType,
    MetaChannel,
    TransportState,
    CONNECT_MESSAGE,
    SUBSCRIBE_MESSAGE,
    DISCONNECT_MESSAGE,
    PUBLISH_MESSAGE,
    UNSUBSCRIBE_MESSAGE,
)
from aiocometd.extensions import Extension, AuthExtension
from aiocometd.exceptions import TransportInvalidOperation, TransportError
from aiocometd.typing import (
    Headers,
    JsonObject,
    Payload,
)


class TransportBaseImpl(TransportBase):
    async def _send_final_payload(
        self, payload: Payload, *, headers: Headers
    ) -> JsonObject:
        return {}

    @property
    def connection_type(self):
        return ConnectionType.LONG_POLLING


@pytest.fixture
async def mock_http_session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def mock_transport_base(mock_http_session):
    return TransportBaseImpl(
        url="example.com/cometd",
        incoming_queue=asyncio.Queue(),
        http_session=mock_http_session,
    )


# TestTransportBase
@pytest.fixture
def long_task():
    def helper(result, timeout=None):
        if not isinstance(result, Exception):
            return result

        raise result

    return helper


@pytest.fixture
def is_event_message():
    with mock.patch(
        "aiocometd.transports.base.is_event_message"
    ) as mock_function:
        yield mock_function


@pytest.fixture
def is_matching_response():
    with mock.patch(
        "aiocometd.transports.base.is_matching_response"
    ) as mock_function:
        yield mock_function


@pytest.fixture
def mock_defer():
    with mock.patch("aiocometd.transports.base.defer") as mock_function:
        yield mock_function


@pytest.fixture
def mock_is_auth_error_message():
    with mock.patch(
        "aiocometd.transports.base.is_auth_error_message"
    ) as mock_function:
        yield mock_function


def test_init_transport_base(mock_transport_base):
    assert mock_transport_base.state == TransportState.DISCONNECTED


def test_init_with_reconnect_advice(mock_transport_base):
    advice = object()
    mock_transport_base._reconnect_advice = advice

    assert mock_transport_base.reconnect_advice is advice


def test_init_without_reconnect_advice(mock_transport_base):
    assert mock_transport_base.reconnect_advice == {}


def test_finalize_message_updates_fields(mock_transport_base):
    message = {
        "field": "value",
        "id": None,
        "clientId": None,
        "connectionType": None,
    }
    mock_transport_base._client_id = "client_id"

    mock_transport_base._finalize_message(message)

    assert message["id"] == str(0)
    assert mock_transport_base._message_id == 1
    assert message["clientId"] == mock_transport_base.client_id
    assert (
        message["connectionType"] == mock_transport_base.connection_type.value
    )


def test_finalize_message_ignores_non_existing_fields(mock_transport_base):
    message = {"field": "value"}
    mock_transport_base._client_id = "client_id"

    mock_transport_base._finalize_message(message)

    assert list(message.keys()) == ["field"]
    assert message["field"] == "value"


def test_finalize_payload_single_message(mock_transport_base):
    payload = {"field": "value", "id": None, "clientId": None}
    mock_transport_base._finalize_message = mock.MagicMock()

    mock_transport_base._finalize_payload(payload)

    mock_transport_base._finalize_message.assert_called_once_with(payload)


def test_finalize_payload_multiple_messages(mock_transport_base):
    payload = [
        {
            "field": "value",
            "id": None,
            "clientId": None,
            "connectionType": None,
        },
        {
            "field2": "value2",
            "id": None,
            "clientId": None,
            "connectionType": None,
        },
    ]
    mock_transport_base._finalize_message = mock.MagicMock()

    mock_transport_base._finalize_payload(payload)

    mock_transport_base._finalize_message.assert_has_calls(
        [mock.call(payload[0]), mock.call(payload[1])]
    )


async def test_consume_message_non_event_message(
    mock_transport_base, is_event_message
):
    is_event_message.return_value = False
    mock_transport_base.incoming_queue = mock.MagicMock()
    mock_transport_base.incoming_queue.put = mock.AsyncMock()
    response_message = object()

    await mock_transport_base._consume_message(response_message)

    is_event_message.assert_called_with(response_message)
    mock_transport_base.incoming_queue.put.assert_not_awaited()


async def test_consume_message_event_message(
    mock_transport_base, is_event_message
):
    is_event_message.return_value = True
    mock_transport_base.incoming_queue = mock.MagicMock()
    mock_transport_base.incoming_queue.put = mock.AsyncMock()
    response_message = object()

    await mock_transport_base._consume_message(response_message)

    is_event_message.assert_called_with(response_message)


def test_update_subscriptions_new_subscription_success(mock_transport_base):
    response_message = {
        "channel": MetaChannel.SUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": True,
        "id": "3",
    }
    mock_transport_base._subscriptions = set()

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set(
        [response_message["subscription"]]
    )


def test_update_subscriptions_existing_subscription_success(
    mock_transport_base,
):
    response_message = {
        "channel": MetaChannel.SUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": True,
        "id": "3",
    }
    mock_transport_base._subscriptions = set(
        [response_message["subscription"]]
    )

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set(
        [response_message["subscription"]]
    )


def test_update_subscriptions_new_subscription_fail(mock_transport_base):
    response_message = {
        "channel": MetaChannel.SUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": False,
        "id": "3",
    }
    mock_transport_base._subscriptions = set()

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_new_subscription_fail_with_error(
    mock_transport_base,
):
    response_message = {
        "channel": MetaChannel.SUBSCRIBE,
        "error": "403::subscription_invalid",
        "successful": False,
        "id": "3",
    }
    mock_transport_base._subscriptions = set()

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_existing_subscription_fail(mock_transport_base):
    response_message = {
        "channel": MetaChannel.SUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": False,
        "id": "3",
    }
    mock_transport_base._subscriptions = set(
        [response_message["subscription"]]
    )

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_new_unsubscription_success(mock_transport_base):
    response_message = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": True,
        "id": "3",
    }
    mock_transport_base._subscriptions = set()

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_existing_unsubscription_success(
    mock_transport_base,
):
    response_message = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": True,
        "id": "3",
    }
    mock_transport_base._subscriptions = set(
        [response_message["subscription"]]
    )

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_new_unsubscription_fail(mock_transport_base):
    response_message = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": False,
        "id": "3",
    }
    mock_transport_base._subscriptions = set()

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set()


def test_update_subscriptions_existing_unsubscription_fail(
    mock_transport_base,
):
    response_message = {
        "channel": MetaChannel.UNSUBSCRIBE,
        "subscription": "/test/channel1",
        "successful": False,
        "id": "3",
    }
    mock_transport_base._subscriptions = set(
        [response_message["subscription"]]
    )

    mock_transport_base._update_subscriptions(response_message)

    assert mock_transport_base.subscriptions == set(
        [response_message["subscription"]]
    )


async def test_consume_payload_matching_without_advice(
    mock_transport_base, is_matching_response
):
    payload = [{"channel": MetaChannel.CONNECT, "successful": True, "id": "1"}]
    message = object()
    mock_transport_base._update_subscriptions = mock.MagicMock()
    is_matching_response.return_value = True
    mock_transport_base._consume_message = mock.MagicMock()

    result = await mock_transport_base._consume_payload(
        payload, find_response_for=message
    )

    assert result == payload[0]
    assert mock_transport_base.reconnect_advice == {}
    mock_transport_base._update_subscriptions.assert_called_with(payload[0])
    is_matching_response.assert_called_with(payload[0], message)
    mock_transport_base._consume_message.assert_not_called()


async def test_process_incoming_payload(mock_transport_base):
    extension = mock.create_autospec(spec=Extension)
    auth = mock.create_autospec(spec=AuthExtension)
    mock_transport_base._extensions = [extension]
    mock_transport_base._auth = auth
    payload = object()
    headers = object()

    await mock_transport_base._process_incoming_payload(payload, headers)

    extension.incoming.assert_awaited_with(payload, headers)
    auth.incoming.assert_awaited_with(payload, headers)


async def test_consume_payload_matching_without_advice_extension(
    mock_transport_base, is_matching_response
):
    payload = [{"channel": MetaChannel.CONNECT, "successful": True, "id": "1"}]
    message = object()
    mock_transport_base._update_subscriptions = mock.MagicMock()
    is_matching_response.return_value = True
    mock_transport_base._consume_message = mock.MagicMock()
    mock_transport_base._process_incoming_payload = mock.AsyncMock()
    headers = object()

    result = await mock_transport_base._consume_payload(
        payload, headers=headers, find_response_for=message
    )

    assert result == payload[0]
    assert mock_transport_base.reconnect_advice == {}
    mock_transport_base._update_subscriptions.assert_called_with(payload[0])
    is_matching_response.assert_called_with(payload[0], message)
    mock_transport_base._consume_message.assert_not_called()
    mock_transport_base._process_incoming_payload.assert_awaited_with(
        payload, headers
    )


async def test_consume_payload_matching_with_advice(
    mock_transport_base, is_matching_response
):
    payload = [
        {
            "channel": MetaChannel.CONNECT,
            "successful": True,
            "advice": {"interval": 0, "reconnect": "retry"},
            "id": "1",
        }
    ]
    message = object()
    mock_transport_base._update_subscriptions = mock.MagicMock()
    is_matching_response.return_value = True
    mock_transport_base._consume_message = mock.MagicMock()
    mock_transport_base._process_incoming_payload = mock.AsyncMock()

    result = await mock_transport_base._consume_payload(
        payload, find_response_for=message
    )

    assert result == payload[0]
    assert mock_transport_base.reconnect_advice == payload[0]["advice"]
    mock_transport_base._update_subscriptions.assert_called_with(payload[0])
    is_matching_response.assert_called_with(payload[0], message)
    mock_transport_base._consume_message.assert_not_called()
    mock_transport_base._process_incoming_payload.assert_awaited_with(
        payload, None
    )


async def test_consume_payload_non_matching(
    mock_transport_base, is_matching_response
):
    payload = [{"channel": MetaChannel.CONNECT, "successful": True, "id": "1"}]
    message = object()
    mock_transport_base._update_subscriptions = mock.MagicMock()
    is_matching_response.return_value = False
    mock_transport_base._consume_message = mock.AsyncMock()
    mock_transport_base._process_incoming_payload = mock.AsyncMock()

    result = await mock_transport_base._consume_payload(
        payload, find_response_for=message
    )

    assert result is None
    assert mock_transport_base.reconnect_advice == {}
    mock_transport_base._update_subscriptions.assert_called_with(payload[0])
    is_matching_response.assert_called_with(payload[0], message)
    mock_transport_base._consume_message.assert_awaited_with(payload[0])
    mock_transport_base._process_incoming_payload.assert_awaited_with(
        payload, None
    )


async def test_consume_payload_none_matching(
    mock_transport_base, is_matching_response
):
    payload = [{"channel": MetaChannel.CONNECT, "successful": True, "id": "1"}]
    message = None
    mock_transport_base._update_subscriptions = mock.MagicMock()
    is_matching_response.return_value = False
    mock_transport_base._consume_message = mock.AsyncMock()
    mock_transport_base._process_incoming_payload = mock.AsyncMock()

    result = await mock_transport_base._consume_payload(
        payload, find_response_for=message
    )

    assert result is None
    assert mock_transport_base.reconnect_advice == {}
    mock_transport_base._update_subscriptions.assert_called_with(payload[0])
    is_matching_response.assert_called_with(payload[0], message)
    mock_transport_base._consume_message.assert_awaited_with(payload[0])
    mock_transport_base._process_incoming_payload.assert_awaited_with(
        payload, None
    )


async def test_send_message(mock_transport_base):
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": None,
        "id": "1",
    }
    response = object()
    mock_transport_base._send_payload_with_auth = mock.AsyncMock(
        return_value=response
    )

    result = await mock_transport_base._send_message(message, field="value")

    assert result is response
    assert message["field"] == "value"
    mock_transport_base._send_payload_with_auth.assert_awaited_with([message])


async def test_handshake(mock_transport_base):
    connection_types = [ConnectionType.WEBSOCKET]
    response = {"clientId": "id1", "successful": True}
    mock_transport_base._send_message = mock.AsyncMock(return_value=response)
    message = {
        "channel": MetaChannel.HANDSHAKE,
        "version": "1.0",
        "supportedConnectionTypes": None,
        "minimumVersion": "1.0",
        "id": None,
    }
    mock_transport_base._subscribe_on_connect = False

    result = await mock_transport_base.handshake(connection_types)

    assert result == response
    final_connection_types = [
        ConnectionType.WEBSOCKET.value,
        mock_transport_base.connection_type.value,
    ]
    mock_transport_base._send_message.assert_awaited_with(
        message, supportedConnectionTypes=final_connection_types
    )
    assert mock_transport_base.client_id == response["clientId"]
    assert mock_transport_base._subscribe_on_connect == True


async def test_handshake_empty_connection_types(mock_transport_base):
    connection_types = []
    response = {"clientId": "id1", "successful": True}
    mock_transport_base._send_message = mock.AsyncMock(return_value=response)
    message = {
        "channel": MetaChannel.HANDSHAKE,
        "version": "1.0",
        "supportedConnectionTypes": None,
        "minimumVersion": "1.0",
        "id": None,
    }
    mock_transport_base._subscribe_on_connect = False

    result = await mock_transport_base.handshake(connection_types)

    assert result == response
    final_connection_types = [mock_transport_base.connection_type.value]
    mock_transport_base._send_message.assert_awaited_with(
        message, supportedConnectionTypes=final_connection_types
    )
    assert mock_transport_base.client_id == response["clientId"]
    assert mock_transport_base._subscribe_on_connect == True


async def test_handshake_with_own_connection_type(mock_transport_base):
    connection_types = [mock_transport_base.connection_type]
    response = {"clientId": "id1", "successful": True}
    mock_transport_base._send_message = mock.AsyncMock(return_value=response)
    message = {
        "channel": MetaChannel.HANDSHAKE,
        "version": "1.0",
        "supportedConnectionTypes": None,
        "minimumVersion": "1.0",
        "id": None,
    }
    mock_transport_base._subscribe_on_connect = False

    result = await mock_transport_base.handshake(connection_types)

    assert result == response
    final_connection_types = [mock_transport_base.connection_type.value]
    mock_transport_base._send_message.assert_awaited_with(
        message, supportedConnectionTypes=final_connection_types
    )
    assert mock_transport_base.client_id == response["clientId"]
    assert mock_transport_base._subscribe_on_connect == True


async def test_handshake_failure(mock_transport_base):
    connection_types = [ConnectionType.WEBSOCKET]
    response = {"clientId": "id1", "successful": False}
    mock_transport_base._send_message = mock.AsyncMock(return_value=response)
    message = {
        "channel": MetaChannel.HANDSHAKE,
        "version": "1.0",
        "supportedConnectionTypes": None,
        "minimumVersion": "1.0",
        "id": None,
    }
    mock_transport_base._subscribe_on_connect = False

    result = await mock_transport_base.handshake(connection_types)

    assert result == response
    final_connection_types = [
        ConnectionType.WEBSOCKET.value,
        mock_transport_base.connection_type.value,
    ]
    mock_transport_base._send_message.assert_awaited_with(
        message, supportedConnectionTypes=final_connection_types
    )
    assert mock_transport_base.client_id == None
    assert mock_transport_base._subscribe_on_connect == False


def test_subscriptions(mock_transport_base):
    assert (
        mock_transport_base.subscriptions is mock_transport_base._subscriptions
    )


def test_subscriptions_read_only(mock_transport_base):
    with pytest.raises(AttributeError):
        mock_transport_base.subscriptions = {"channel1", "channel2"}


async def test__connect(mock_transport_base):
    mock_transport_base._subscribe_on_connect = False
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": True,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    mock_transport_base._send_payload_with_auth = mock.AsyncMock(
        return_value=response
    )

    result = await mock_transport_base._connect()

    assert result == response
    mock_transport_base._send_payload_with_auth.assert_awaited_with(
        [CONNECT_MESSAGE]
    )
    assert mock_transport_base._subscribe_on_connect == False


async def test__connect_subscribe_on_connect(mock_transport_base):
    mock_transport_base._subscribe_on_connect = True
    mock_transport_base._subscriptions = {"/test/channel1", "/test/channel2"}
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": True,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    additional_messages = []
    for subscription in mock_transport_base.subscriptions:
        message = SUBSCRIBE_MESSAGE.copy()
        message["subscription"] = subscription
        additional_messages.append(message)
    mock_transport_base._send_payload_with_auth = mock.AsyncMock(
        return_value=response
    )

    result = await mock_transport_base._connect()

    assert result == response
    mock_transport_base._send_payload_with_auth.assert_awaited_with(
        [CONNECT_MESSAGE] + additional_messages
    )
    assert mock_transport_base._subscribe_on_connect == False


async def test__connect_subscribe_on_connect_error(mock_transport_base):
    mock_transport_base._subscribe_on_connect = True
    mock_transport_base._subscriptions = {"/test/channel1", "/test/channel2"}
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": False,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    additional_messages = []
    for subscription in mock_transport_base.subscriptions:
        message = SUBSCRIBE_MESSAGE.copy()
        message["subscription"] = subscription
        additional_messages.append(message)
    mock_transport_base._send_payload_with_auth = mock.AsyncMock(
        return_value=response
    )

    result = await mock_transport_base._connect()

    assert result == response
    mock_transport_base._send_payload_with_auth.assert_awaited_with(
        [CONNECT_MESSAGE] + additional_messages
    )
    assert mock_transport_base._subscribe_on_connect == True


def test__state_initially_disconnected(mock_transport_base):
    assert mock_transport_base._state is TransportState.DISCONNECTED


def test_state(mock_transport_base):
    assert mock_transport_base.state is mock_transport_base._state


def test_state_read_only(mock_transport_base):
    with pytest.raises(AttributeError):
        mock_transport_base.state = TransportState.CONNECTING


async def test_connect_error_on_invalid_state(mock_transport_base):
    mock_transport_base._client_id = "id"
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": True,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    mock_transport_base._connect = mock.AsyncMock(return_value=response)
    for state in TransportState:
        if state not in [
            TransportState.DISCONNECTED,
            TransportState.SERVER_DISCONNECTED,
        ]:
            mock_transport_base._state = state
            with pytest.raises(
                TransportInvalidOperation,
                match="Can't connect to a server without disconnecting first.",
            ):
                await mock_transport_base.connect()


async def test_connect_error_on_invalid_client_id(mock_transport_base):
    mock_transport_base._client_id = ""

    with pytest.raises(
        TransportInvalidOperation,
        match="Can't connect to the server without a client id. Do a handshake first.",
    ):
        await mock_transport_base.connect()


async def test_connect(mock_transport_base):
    mock_transport_base._client_id = "id"
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": True,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    mock_transport_base._connect = mock.AsyncMock(return_value=response)
    mock_transport_base._connect_done = mock.MagicMock()
    mock_transport_base._state = TransportState.DISCONNECTED

    result = await mock_transport_base.connect()

    assert result == response
    assert mock_transport_base.state == TransportState.CONNECTING


async def test_connect_in_server_disconnected_state(mock_transport_base):
    mock_transport_base._client_id = "id"
    response = {
        "channel": MetaChannel.CONNECT,
        "successful": True,
        "advice": {"interval": 0, "reconnect": "retry"},
        "id": "2",
    }
    mock_transport_base._connect = mock.AsyncMock(return_value=response)
    mock_transport_base._connect_done = mock.MagicMock()
    mock_transport_base._state = TransportState.SERVER_DISCONNECTED

    result = await mock_transport_base.connect()

    assert result == response
    assert mock_transport_base.state == TransportState.CONNECTING


@pytest.mark.parametrize(
    ["result", "expected_advice"],
    (
        ({"successful": True}, "retry"),
        ({"successful": False}, "retry"),
        (
            {"successful": False, "advice": {}},
            "retry",
        ),
        (
            {"successful": False, "advice": {"reconnect": "none"}},
            "none",
        ),
    ),
)
async def test_connect_done_with_result(
    mock_transport_base, result, expected_advice, caplog
):
    caplog.set_level(logging.DEBUG)

    loop = asyncio.get_running_loop()
    task = loop.create_future()
    task.set_result(result)
    mock_transport_base._reconnect_advice = {"interval": 1}
    mock_transport_base._follow_advice = mock.MagicMock()
    mock_transport_base._state = TransportState.CONNECTING
    mock_transport_base._reconnect_timeout = 2

    mock_transport_base._connect_done(task)

    log_message = "Connect task finished with: {!r}".format(result)
    assert log_message in caplog.messages

    mock_transport_base._follow_advice.assert_called_with(expected_advice, 1)
    assert mock_transport_base.state == TransportState.CONNECTED


async def test_connect_done_with_error(mock_transport_base, caplog):
    caplog.set_level(logging.DEBUG)

    error = RuntimeError("error")
    loop = asyncio.get_running_loop()
    task = loop.create_future()
    task.set_exception(error)
    mock_transport_base._follow_advice = mock.MagicMock()
    mock_transport_base._state = TransportState.CONNECTED
    mock_transport_base._reconnect_timeout = 2

    mock_transport_base._connect_done(task)

    log_message = "Connect task finished with: {!r}".format(error)
    assert log_message in caplog.messages

    mock_transport_base._follow_advice.assert_called_with("retry", 2)
    assert mock_transport_base.state == TransportState.CONNECTING


async def test_connect_dont_follow_advice_on_disconnecting(
    mock_transport_base, caplog
):
    caplog.set_level(logging.DEBUG)

    error = RuntimeError("error")
    loop = asyncio.get_running_loop()
    task = loop.create_future()
    task.set_exception(error)
    mock_transport_base._follow_advice = mock.MagicMock()
    mock_transport_base._state = TransportState.DISCONNECTING
    mock_transport_base._reconnect_advice = {
        "interval": 1,
        "reconnect": "retry",
    }
    mock_transport_base._reconnect_timeout = 2

    mock_transport_base._connect_done(task)

    log_message = "Connect task finished with: {!r}".format(error)
    assert log_message in caplog.messages

    mock_transport_base._follow_advice.assert_not_called()


def test_follow_advice_handshake(mock_transport_base, mock_defer):
    mock_transport_base.handshake = mock.MagicMock(return_value=object())
    mock_transport_base._connect = mock.MagicMock(return_value=object())
    mock_transport_base._start_connect_task = mock.MagicMock()
    mock_defer.return_value = mock.MagicMock(return_value=object())

    mock_transport_base._follow_advice("handshake", 5)

    mock_defer.assert_called_with(mock_transport_base.handshake, delay=5)
    mock_defer.return_value.assert_called_with(
        [mock_transport_base.connection_type]
    )
    mock_transport_base._connect.assert_not_called()
    mock_transport_base._start_connect_task.assert_called_with(
        mock_defer.return_value.return_value
    )


def test_follow_advice_retry(mock_transport_base, mock_defer):
    mock_transport_base.handshake = mock.MagicMock(return_value=object())
    mock_transport_base._connect = mock.MagicMock(return_value=object())
    mock_transport_base._start_connect_task = mock.MagicMock()
    mock_defer.return_value = mock.MagicMock(return_value=object())

    mock_transport_base._follow_advice("retry", 5)

    mock_defer.assert_called_with(mock_transport_base._connect, delay=5)
    mock_defer.return_value.assert_called()
    mock_transport_base.handshake.assert_not_called()
    mock_transport_base._start_connect_task.assert_called_with(
        mock_defer.return_value.return_value
    )


@pytest.mark.parametrize(
    "advice",
    (
        ["none"],
        [""],
        [None],
    ),
)
def test_follow_advice_none(mock_transport_base, mock_defer, caplog, advice):
    caplog.set_level(logging.WARN)

    mock_transport_base._state = TransportState.CONNECTED
    mock_transport_base.handshake = mock.MagicMock(return_value=object())
    mock_transport_base._connect = mock.MagicMock(return_value=object())
    mock_transport_base._start_connect_task = mock.MagicMock()
    mock_defer.return_value = mock.MagicMock()

    mock_transport_base._follow_advice(advice, 5)

    expected_log = "No reconnect advice provided, no more operations will be scheduled.".format(
        TransportBase.__module__
    )
    assert expected_log in caplog.messages

    mock_defer.assert_not_called()
    mock_transport_base.handshake.assert_not_called()
    mock_transport_base._connect.assert_not_called()
    mock_transport_base._start_connect_task.assert_not_called()
    assert mock_transport_base.state == TransportState.SERVER_DISCONNECTED


def test_client_id(mock_transport_base):
    assert mock_transport_base.client_id is mock_transport_base._client_id


def test_client_id_read_only(mock_transport_base):
    with pytest.raises(AttributeError):
        mock_transport_base.client_id = "id"


def test_endpoint(mock_transport_base):
    assert mock_transport_base.endpoint is mock_transport_base._url


def test_endpoint_read_only(mock_transport_base):
    with pytest.raises(AttributeError):
        mock_transport_base.endpoint = ""


async def test_disconnect(mock_transport_base):
    mock_transport_base._state = TransportState.CONNECTED
    mock_transport_base._stop_connect_task = mock.AsyncMock()
    mock_transport_base._send_message = mock.AsyncMock()

    await mock_transport_base.disconnect()

    assert mock_transport_base.state == TransportState.DISCONNECTED
    mock_transport_base._stop_connect_task.assert_called()
    mock_transport_base._send_message.assert_awaited_with(DISCONNECT_MESSAGE)


async def test_disconnect_transport_error(mock_transport_base):
    mock_transport_base._state = TransportState.CONNECTED
    mock_transport_base._stop_connect_task = mock.AsyncMock()
    mock_transport_base._send_message = mock.AsyncMock(
        side_effect=TransportError()
    )

    await mock_transport_base.disconnect()

    assert mock_transport_base.state == TransportState.DISCONNECTED
    mock_transport_base._stop_connect_task.assert_awaited()
    mock_transport_base._send_message.assert_awaited_with(DISCONNECT_MESSAGE)


@pytest.mark.parametrize(
    "state",
    [state for state in TransportState if state != TransportState.CONNECTED],
)
async def test_disconnect_if_not_connected(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._stop_connect_task = mock.AsyncMock()
    mock_transport_base._send_message = mock.AsyncMock()

    await mock_transport_base.disconnect()

    assert mock_transport_base.state == TransportState.DISCONNECTED
    mock_transport_base._stop_connect_task.assert_called()
    mock_transport_base._send_message.assert_not_awaited()


@pytest.mark.parametrize(
    "state", [TransportState.CONNECTED, TransportState.CONNECTING]
)
async def test_subscribe(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    result = await mock_transport_base.subscribe("channel")

    assert result == mock_transport_base._send_message.return_value
    mock_transport_base._send_message.assert_awaited_with(
        SUBSCRIBE_MESSAGE, subscription="channel"
    )


@pytest.mark.parametrize(
    "state",
    [
        state
        for state in TransportState
        if state != TransportState.CONNECTED
        and state != TransportState.CONNECTING
    ],
)
async def test_subscribe_error_if_not_connected(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    with pytest.raises(
        TransportInvalidOperation,
        match="Can't subscribe without being connected to a server.",
    ):
        await mock_transport_base.subscribe("channel")

    mock_transport_base._send_message.assert_not_awaited()


@pytest.mark.parametrize(
    "state", [TransportState.CONNECTED, TransportState.CONNECTING]
)
async def test_unsubscribe(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    result = await mock_transport_base.unsubscribe("channel")

    assert result == mock_transport_base._send_message.return_value
    mock_transport_base._send_message.assert_awaited_with(
        UNSUBSCRIBE_MESSAGE, subscription="channel"
    )


@pytest.mark.parametrize(
    "state",
    [
        state
        for state in TransportState
        if state != TransportState.CONNECTED
        and state != TransportState.CONNECTING
    ],
)
async def test_unsubscribe_error_if_not_connected(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    with pytest.raises(
        TransportInvalidOperation,
        match="Can't unsubscribe without being connected to a server.",
    ):
        await mock_transport_base.unsubscribe("channel")

    mock_transport_base._send_message.assert_not_awaited()


@pytest.mark.parametrize(
    "state", [TransportState.CONNECTED, TransportState.CONNECTING]
)
async def test_publish(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    result = await mock_transport_base.publish("channel", {})

    assert result == mock_transport_base._send_message.return_value
    mock_transport_base._send_message.assert_awaited_with(
        PUBLISH_MESSAGE, channel="channel", data={}
    )


@pytest.mark.parametrize(
    "state",
    [
        state
        for state in TransportState
        if state != TransportState.CONNECTED
        and state != TransportState.CONNECTING
    ],
)
async def test_publish_error_if_not_connected(mock_transport_base, state):
    mock_transport_base._state = state
    mock_transport_base._send_message = mock.AsyncMock(return_value="result")

    with pytest.raises(
        TransportInvalidOperation,
        match="Can't publish without being connected to a server.",
    ):
        await mock_transport_base.publish("channel", {})

    mock_transport_base._send_message.assert_not_awaited()


async def test_wait_for_state(mock_transport_base):
    state = TransportState.CONNECTING
    event = mock_transport_base._state_events[state]
    event.wait = mock.AsyncMock()

    await mock_transport_base.wait_for_state(state)

    event.wait.assert_awaited()


async def test_send_payload(mock_transport_base):
    payload = object()
    mock_transport_base._finalize_payload = mock.MagicMock()
    response = object()
    mock_transport_base._send_final_payload = mock.AsyncMock(
        return_value=response
    )
    mock_transport_base._process_outgoing_payload = mock.AsyncMock()

    result = await mock_transport_base._send_payload(payload)

    assert result == response
    mock_transport_base._finalize_payload.assert_called_with(payload)
    mock_transport_base._send_final_payload.assert_awaited_with(
        payload, headers={}
    )
    mock_transport_base._process_outgoing_payload.assert_awaited_with(
        payload, {}
    )


async def test_process_outgoing_payload(mock_transport_base):
    extension = mock.create_autospec(spec=Extension)
    auth = mock.create_autospec(spec=AuthExtension)
    mock_transport_base._extensions = [extension]
    mock_transport_base._auth = auth
    payload = object()
    headers = object()

    await mock_transport_base._process_outgoing_payload(payload, headers)

    extension.outgoing.assert_called_with(payload, headers)
    auth.outgoing.assert_called_with(payload, headers)


async def test_process_outgoing_payload_without_auth(mock_transport_base):
    extension = mock.create_autospec(spec=Extension)
    mock_transport_base._extensions = [extension]
    mock_transport_base._auth = None
    payload = object()
    headers = object()

    await mock_transport_base._process_outgoing_payload(payload, headers)

    extension.outgoing.assert_called_with(payload, headers)


async def test_send_payload_with_auth(mock_transport_base):
    response = object()
    payload = object()
    mock_transport_base._send_payload = mock.AsyncMock(return_value=response)
    mock_transport_base._auth = None
    mock_transport_base._is_auth_error_message = mock.MagicMock(
        return_value=False
    )

    result = await mock_transport_base._send_payload_with_auth(payload)

    assert result is response
    mock_transport_base._send_payload.assert_awaited_with(payload)
    mock_transport_base._is_auth_error_message.assert_not_called()


async def test_send_payload_with_auth_with_extension(
    mock_transport_base, mock_is_auth_error_message
):
    response = object()
    payload = object()
    mock_transport_base._send_payload = mock.AsyncMock(return_value=response)
    mock_transport_base._auth = mock.create_autospec(spec=AuthExtension)
    mock_is_auth_error_message.return_value = False

    result = await mock_transport_base._send_payload_with_auth(payload)

    assert result is response
    mock_transport_base._send_payload.assert_awaited_with(payload)
    mock_is_auth_error_message.assert_called_with(response)
    mock_transport_base._auth.authenticate.assert_not_called()


async def test_send_payload_with_auth_with_extension_error(
    mock_transport_base, mock_is_auth_error_message
):
    response = object()
    response2 = object()
    payload = object()
    mock_transport_base._send_payload = mock.AsyncMock(
        side_effect=[response, response2]
    )
    mock_transport_base._auth = mock.create_autospec(spec=AuthExtension)
    mock_is_auth_error_message.return_value = True

    result = await mock_transport_base._send_payload_with_auth(payload)

    assert result is response2
    mock_transport_base._send_payload.assert_has_awaits(
        [mock.call(payload), mock.call(payload)]
    )
    mock_is_auth_error_message.assert_called_with(response)
    mock_transport_base._auth.authenticate.assert_called()


def test_state_sunder(mock_transport_base):
    result = mock_transport_base._state

    assert result == mock_transport_base.__dict__["_state"]


def test_state_sunder_setter(mock_transport_base):
    state = TransportState.CONNECTED
    assert mock_transport_base.__dict__["_state"] != state

    old_state = mock_transport_base._state
    mock_transport_base._set_state_event = mock.MagicMock()

    mock_transport_base._state = state

    assert mock_transport_base.__dict__["_state"] == state
    mock_transport_base._set_state_event.assert_called_with(old_state, state)


def test_set_state_event(mock_transport_base):
    old_state = TransportState.DISCONNECTED
    new_state = TransportState.CONNECTED
    mock_transport_base._state_events[old_state].set()
    mock_transport_base._state_events[new_state].clear()

    mock_transport_base._set_state_event(old_state, new_state)

    assert mock_transport_base._state_events[old_state].is_set() == False
    assert mock_transport_base._state_events[new_state].is_set() == True


def test_set_state_event_unchanged_state(mock_transport_base):
    state = TransportState.CONNECTED
    event_mock = mock.MagicMock()
    mock_transport_base._state_events[state] = event_mock

    mock_transport_base._set_state_event(state, state)

    event_mock.set.assert_not_called()
    event_mock.clear.assert_not_called()


def test_last_connect_result(mock_transport_base):
    mock_transport_base._connect_task = mock.MagicMock()
    mock_transport_base._connect_task.done.return_value = True
    mock_transport_base._connect_task.result.return_value = object()

    result = mock_transport_base.last_connect_result

    assert result is mock_transport_base._connect_task.result.return_value


def test_last_connect_result_on_no_connect_task(mock_transport_base):
    mock_transport_base._connect_task = None

    result = mock_transport_base.last_connect_result

    assert result is None


def test_request_timeout_int(mock_transport_base):
    mock_transport_base._reconnect_advice = {"timeout": 2000}

    expected = (
        mock_transport_base.reconnect_advice["timeout"] / 1000
    ) * mock_transport_base.REQUEST_TIMEOUT_INCREASE_FACTOR
    assert mock_transport_base.request_timeout == expected


def test_request_timeout_float(mock_transport_base):
    mock_transport_base._reconnect_advice = {"timeout": 2000.0}

    expected = (
        mock_transport_base.reconnect_advice["timeout"] / 1000
    ) * mock_transport_base.REQUEST_TIMEOUT_INCREASE_FACTOR
    assert mock_transport_base.request_timeout == expected


def test_request_timeout_none(mock_transport_base):
    mock_transport_base._reconnect_advice = {}

    assert mock_transport_base.request_timeout is None


def test_request_timeout_none_on_unsupported_timeout_type(mock_transport_base):
    mock_transport_base._reconnect_advice = {"timeout": "2000"}

    assert mock_transport_base.request_timeout is None
