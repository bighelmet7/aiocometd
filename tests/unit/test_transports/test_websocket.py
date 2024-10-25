import asyncio
import logging
from unittest import mock

import pytest
import aiohttp
from aiohttp import client_exceptions, WSMsgType

from aiocometd.transports.websocket import WebSocketTransport
from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportConnectionClosed, TransportError


@pytest.fixture
async def mock_session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def mock_websocket(mock_session):
    # TODO: fix typing
    return WebSocketTransport(
        url="example.com/cometd",
        incoming_queue=asyncio.Queue(),
        http_session=mock_session,
    )


def test_connection_type(mock_websocket, mock_session):
    assert mock_websocket.connection_type == ConnectionType.WEBSOCKET


async def test_get_socket(mock_websocket, mock_session):
    expected_socket = object()
    mock_websocket._socket_factory = mock.AsyncMock(
        return_value=expected_socket
    )
    headers = object()

    result = await mock_websocket._get_socket(headers)

    assert result is expected_socket
    mock_websocket._socket_factory.assert_awaited_with(
        mock_websocket._url,
        ssl=mock_websocket.ssl,
        headers=headers,
        receive_timeout=mock_websocket.request_timeout,
        autoping=True,
    )


async def test_close(mock_websocket):
    mock_websocket._socket_factory_short = mock.MagicMock()
    mock_websocket._socket_factory_short.close = mock.AsyncMock()
    mock_websocket._socket_factory = mock.MagicMock()
    mock_websocket._socket_factory.close = mock.AsyncMock()
    mock_websocket._close_http_session = mock.AsyncMock()
    mock_websocket._receive_task = mock.MagicMock()
    mock_websocket._receive_task.done.return_value = False

    with mock.patch("aiocometd.transports.websocket.asyncio") as asyncio_obj:
        asyncio_obj.wait = mock.AsyncMock()
        await mock_websocket.close()

    mock_websocket._receive_task.cancel.assert_called()
    asyncio_obj.wait.assert_awaited_with([mock_websocket._receive_task])
    mock_websocket._socket_factory.close.assert_awaited()


async def test_close_on_done_receive_task(mock_websocket):
    mock_websocket._socket_factory_short = mock.MagicMock()
    mock_websocket._socket_factory_short.close = mock.AsyncMock()
    mock_websocket._socket_factory = mock.MagicMock()
    mock_websocket._socket_factory.close = mock.AsyncMock()
    mock_websocket._close_http_session = mock.AsyncMock()
    mock_websocket._receive_task = mock.MagicMock()
    mock_websocket._receive_task.done.return_value = True

    with mock.patch("aiocometd.transports.websocket.asyncio") as asyncio_obj:
        asyncio_obj.wait = mock.AsyncMock()
        await mock_websocket.close()

    mock_websocket._receive_task.cancel.assert_not_called()
    asyncio_obj.wait.assert_not_awaited()
    mock_websocket._socket_factory.close.assert_awaited()


async def test_close_on_no_receive_task(mock_websocket):
    mock_websocket._socket_factory_short = mock.MagicMock()
    mock_websocket._socket_factory_short.close = mock.AsyncMock()
    mock_websocket._socket_factory = mock.MagicMock()
    mock_websocket._socket_factory.close = mock.AsyncMock()
    mock_websocket._close_http_session = mock.AsyncMock()
    mock_websocket._receive_task = None

    with mock.patch("aiocometd.transports.websocket.asyncio") as asyncio_obj:
        asyncio_obj.wait = mock.AsyncMock()
        await mock_websocket.close()

    mock_websocket._socket_factory.close.assert_awaited()


async def test_send_socket_payload(mock_websocket, mock_session):
    payload = object()
    socket = mock.MagicMock()
    socket.send_json = mock.AsyncMock()
    expected_result = object()
    future = asyncio.Future()
    future.set_result(expected_result)
    exchange_result = future
    mock_websocket._create_exchange_future = mock.MagicMock(
        return_value=exchange_result
    )
    mock_websocket._start_receive_task = mock.MagicMock()

    result = await mock_websocket._send_socket_payload(socket, payload)

    mock_websocket._create_exchange_future.assert_called_with(payload)
    socket.send_json.assert_awaited_with(
        payload, dumps=mock_websocket._json_dumps
    )
    mock_websocket._start_receive_task.assert_called()
    assert result == expected_result


async def test_send_socket_payload_creates_receive_task(
    mock_websocket, mock_session
):
    payload = object()
    socket = mock.MagicMock()
    socket.send_json = mock.AsyncMock()
    expected_result = object()
    future = asyncio.Future()
    future.set_result(expected_result)
    exchange_result = future
    mock_websocket._create_exchange_future = mock.MagicMock(
        return_value=exchange_result
    )
    mock_websocket._start_receive_task = mock.MagicMock()

    result = await mock_websocket._send_socket_payload(socket, payload)

    mock_websocket._create_exchange_future.assert_called_with(payload)
    socket.send_json.assert_awaited_with(
        payload, dumps=mock_websocket._json_dumps
    )
    mock_websocket._start_receive_task.assert_called()
    assert result == expected_result


async def test_send_socket_payload_on_send_error(mock_websocket, mock_session):
    payload = [{"id": 0}]
    socket = mock.MagicMock()
    error = ValueError()
    socket.send_json = mock.AsyncMock(side_effect=error)
    future = asyncio.Future()
    exchange_result = asyncio.Future()
    exchange_result.set_result(future)
    mock_websocket._create_exchange_future = mock.MagicMock(
        return_value=exchange_result
    )
    mock_websocket._start_receive_task = mock.MagicMock()
    mock_websocket._set_exchange_errors = mock.MagicMock()

    with pytest.raises(ValueError):
        await mock_websocket._send_socket_payload(socket, payload)

    mock_websocket._create_exchange_future.assert_called_with(payload)
    socket.send_json.assert_awaited_with(
        payload, dumps=mock_websocket._json_dumps
    )
    mock_websocket._set_exchange_errors.assert_called_with(error)
    mock_websocket._start_receive_task.assert_not_called()


def test_start_receive_task_if_exists(mock_websocket, mock_session):
    socket = object()
    existing_receive_task = object()
    receive_task = mock.MagicMock()
    mock_websocket._loop = mock.MagicMock()
    mock_websocket._loop.create_task = mock.MagicMock(
        return_value=receive_task
    )
    mock_websocket._receive = mock.MagicMock()
    mock_websocket._receive_task = existing_receive_task

    mock_websocket._start_receive_task(socket)

    mock_websocket._loop.create_task.assert_not_called()
    mock_websocket._receive.assert_not_called()
    receive_task.add_done_callback.assert_not_called()
    assert mock_websocket._receive_task == existing_receive_task


async def test_send_final_payload(mock_websocket, mock_session):
    payload = object()
    socket = object()
    response = object()
    mock_websocket._get_socket = mock.AsyncMock(return_value=socket)
    mock_websocket._send_socket_payload = mock.AsyncMock(return_value=response)
    headers = object()

    result = await mock_websocket._send_final_payload(payload, headers=headers)

    assert result == response
    mock_websocket._get_socket.assert_awaited_with(headers)
    mock_websocket._send_socket_payload.assert_awaited_with(socket, payload)


async def test_send_final_payload_transport_error(
    mock_websocket, mock_session, caplog
):
    caplog.set_level(logging.WARN)

    payload = object()
    socket = object()
    exception = client_exceptions.ClientError("message")
    mock_websocket._get_socket = mock.AsyncMock(return_value=socket)
    mock_websocket._send_socket_payload = mock.AsyncMock(side_effect=exception)
    headers = object()

    with pytest.raises(TransportError, match=str(exception)):
        await mock_websocket._send_final_payload(payload, headers=headers)

    log_message = "Failed to send payload, {}".format(exception)
    assert log_message in caplog.messages
    mock_websocket._get_socket.assert_awaited_with(headers)
    mock_websocket._send_socket_payload.assert_awaited_with(socket, payload)


async def test_send_final_payload_connection_closed_error(
    mock_websocket, mock_session
):
    payload = object()
    socket = object()
    socket2 = object()
    response = object()
    mock_websocket._get_socket = mock.AsyncMock(side_effect=[socket, socket2])
    error = TransportConnectionClosed()
    mock_websocket._send_socket_payload = mock.AsyncMock(
        side_effect=[error, response]
    )
    headers = object()

    result = await mock_websocket._send_final_payload(payload, headers=headers)

    assert result == response
    mock_websocket._get_socket.assert_has_awaits(
        [mock.call(headers), mock.call(headers)]
    )
    mock_websocket._send_socket_payload.assert_has_awaits(
        [mock.call(socket, payload), mock.call(socket2, payload)]
    )


async def test_send_final_payload_connection_timeout_error(
    mock_websocket, mock_session
):
    payload = object()
    socket = object()
    mock_websocket._get_socket = mock.AsyncMock(return_value=socket)
    error = asyncio.TimeoutError()
    mock_websocket._send_socket_payload = mock.AsyncMock(side_effect=error)
    headers = object()
    mock_websocket._reset_socket = mock.AsyncMock()

    with pytest.raises(asyncio.TimeoutError):
        await mock_websocket._send_final_payload(payload, headers=headers)

    mock_websocket._get_socket.assert_awaited_with(headers)
    mock_websocket._send_socket_payload.assert_awaited_with(socket, payload)
    mock_websocket._reset_socket.assert_awaited()


async def test_reset_socket(mock_websocket):
    old_factory = mock.MagicMock()
    old_factory.close = mock.AsyncMock()
    mock_websocket._socket_factory = old_factory

    with mock.patch(
        "aiocometd.transports.websocket.WebSocketFactory"
    ) as ws_factory_cls:
        socket_factory = object()
        ws_factory_cls.return_value = socket_factory
        await mock_websocket._reset_socket()

        old_factory.close.assert_called()
        assert mock_websocket._socket_factory is socket_factory
        ws_factory_cls.assert_called_with(mock_websocket._http_session)


async def test_create_exchange_future(mock_websocket):
    payload = [{"id": 42}]

    with mock.patch(
        "aiocometd.transports.websocket.asyncio.Future"
    ) as future_cls:
        future = object()
        future_cls.return_value = future
        result = mock_websocket._create_exchange_future(payload)

        assert result == future
        assert mock_websocket._pending_exchanges == {42: future}


async def test_receive_done_with_result(mock_websocket, mock_session, caplog):
    caplog.set_level(logging.DEBUG)

    future = mock.MagicMock()
    result = object()
    future.result.return_value = result
    mock_websocket._receive_task = object()

    mock_websocket._receive_done(future)

    mock_websocket._receive_task = None

    excepted_log = f"Recevie task finished with: {result!r}"
    assert excepted_log in caplog.messages


async def test_receive_done_with_error(mock_websocket, mock_session, caplog):
    caplog.set_level(logging.DEBUG)

    future = mock.MagicMock()
    result = ValueError()
    future.result.side_effect = result
    mock_websocket._receive_task = object()

    mock_websocket._receive_done(future)

    mock_websocket._receive_task = None
    expected_log = f"Recevie task finished with: {result!r}"
    assert expected_log in caplog.messages


def test_set_exchange_errors(mock_websocket, mock_session):
    error = ValueError()
    future = asyncio.Future()
    mock_websocket._pending_exchanges = {0: future}

    mock_websocket._set_exchange_errors(error)

    assert future.exception() == error
    assert mock_websocket._pending_exchanges == dict()


def test_set_exchange_errors_skips_completed_futures(
    mock_websocket, mock_session
):
    error = ValueError()
    result = object()
    future = asyncio.Future()
    future.set_result(result)
    mock_websocket._pending_exchanges = {0: future}

    mock_websocket._set_exchange_errors(error)

    assert future.result() == result
    assert mock_websocket._pending_exchanges == dict()


def test_set_exchange_results(mock_websocket, mock_session):
    future1 = asyncio.Future()
    future2 = asyncio.Future()
    future2_result = object()
    future2.set_result(future2_result)
    future3 = asyncio.Future()
    mock_websocket._pending_exchanges = {0: future1, 1: future2, 3: future3}
    payload = [{"id": 0}, {"id": 1}, {"id": 2}, {}]

    mock_websocket._set_exchange_results(payload)
    assert future1.result() == payload[0]
    assert future2.result() == future2_result
    assert mock_websocket._pending_exchanges == {3: future3}


async def test_receive(mock_websocket, mock_session):
    response = mock.MagicMock()
    response_payload = object()
    response.json.return_value = response_payload
    response.type = aiohttp.WSMsgType.PING
    socket = mock.MagicMock()
    socket.receive = mock.AsyncMock(
        side_effect=[response, Exception],
    )
    mock_websocket._consume_payload = mock.AsyncMock()
    mock_websocket._set_exchange_results = mock.MagicMock()

    with pytest.raises(Exception):
        await mock_websocket._receive(socket)

    socket.receive.assert_awaited()
    response.json.assert_called_with(loads=mock_websocket._json_loads)
    mock_websocket._consume_payload.assert_awaited_with(response_payload)
    mock_websocket._set_exchange_results.assert_called_with(response_payload)


async def test_receive_socket_closed(mock_websocket, mock_session):
    response = mock.MagicMock()
    response.type = WSMsgType.CLOSE
    response_payload = object()
    response.json.return_value = response_payload
    socket = mock.MagicMock()
    socket.receive = mock.AsyncMock(return_value=response)
    mock_websocket._consume_payload = mock.AsyncMock()
    mock_websocket._set_exchange_results = mock.AsyncMock()

    with pytest.raises(
        TransportConnectionClosed,
        match="Received CLOSE message on the factory.",
    ):
        await mock_websocket._receive(socket)

    socket.receive.assert_awaited()
    response.json.assert_not_called()
    mock_websocket._consume_payload.assert_not_awaited()
    mock_websocket._set_exchange_results.assert_not_awaited()


async def test_receive_parse_type_error(mock_websocket, mock_session):
    response = mock.MagicMock()
    response.json.side_effect = TypeError()
    socket = mock.MagicMock()
    socket.receive = mock.AsyncMock(
        side_effect=[response, asyncio.CancelledError()]
    )
    mock_websocket._consume_payload = mock.AsyncMock()
    mock_websocket._set_exchange_results = mock.AsyncMock()

    with pytest.raises(
        TransportError, match="Received invalid response from the server."
    ):
        await mock_websocket._receive(socket)

    socket.receive.assert_awaited()
    response.json.assert_called_with(loads=mock_websocket._json_loads)
    mock_websocket._consume_payload.assert_not_awaited()
    mock_websocket._set_exchange_results.assert_not_awaited()


async def test_receive_any_error(mock_websocket, mock_session):
    response = mock.MagicMock()
    response_payload = object()
    response.json.return_value = response_payload
    socket = mock.MagicMock()
    error = ValueError()
    socket.receive = mock.AsyncMock(side_effect=error)
    mock_websocket._consume_payload = mock.AsyncMock()
    mock_websocket._set_exchange_results = mock.AsyncMock()

    with pytest.raises(ValueError):
        await mock_websocket._receive(socket)

    socket.receive.assert_awaited()
    response.json.assert_not_called()
    mock_websocket._consume_payload.assert_not_awaited()
    mock_websocket._set_exchange_results.assert_not_awaited()
