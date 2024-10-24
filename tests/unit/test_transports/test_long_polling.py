import asyncio
import logging
from unittest import mock

import aiohttp
import pytest
from aiohttp import client_exceptions

from aiocometd.transports.long_polling import LongPollingTransport
from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportError


@pytest.fixture
async def mock_http_session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def mock_transport(mock_http_session):
    # TODO: fix typing
    return LongPollingTransport(
        url="example.com/cometd",
        incoming_queue=asyncio.Queue(),
        http_session=mock_http_session,
    )


def test_connection_type(mock_transport):
    assert mock_transport.connection_type == ConnectionType.LONG_POLLING


async def test_send_payload_final_payload(mock_transport):
    resp_data = [{"channel": "test/channel3", "data": {}, "id": 4}]
    response_mock = mock.MagicMock()
    response_mock.json = mock.AsyncMock(return_value=resp_data)
    response_mock.headers = object()
    session = mock.MagicMock()
    session.post = mock.AsyncMock(return_value=response_mock)
    mock_transport._http_session = session
    mock_transport._http_semaphore = mock.MagicMock()
    payload = [object(), object()]
    mock_transport.ssl = object()
    mock_transport._consume_payload = mock.AsyncMock(return_value=resp_data[0])
    headers = dict(key="value")

    response = await mock_transport._send_final_payload(
        payload, headers=headers
    )

    assert response == resp_data[0]
    mock_transport._http_semaphore.__aenter__.assert_awaited()
    mock_transport._http_semaphore.__aexit__.assert_awaited()
    session.post.assert_awaited_with(
        mock_transport._url,
        json=payload,
        ssl=mock_transport.ssl,
        headers=headers,
        timeout=mock_transport.request_timeout,
    )
    response_mock.json.assert_awaited_with(loads=mock_transport._json_loads)
    mock_transport._consume_payload.assert_awaited_with(
        resp_data,
        headers=response_mock.headers,
        find_response_for=payload[0],
    )


async def test_send_payload_final_payload_client_error(mock_transport, caplog):
    caplog.set_level(logging.WARN)

    resp_data = [{"channel": "test/channel3", "data": {}, "id": 4}]
    response_mock = mock.MagicMock()
    response_mock.json = mock.AsyncMock(return_value=resp_data)
    session = mock.MagicMock()
    post_exception = client_exceptions.ClientError("client error")
    session.post = mock.AsyncMock(side_effect=post_exception)
    mock_transport._http_session = session
    mock_transport._http_semaphore = mock.MagicMock()
    payload = [object(), object()]
    mock_transport.ssl = object()
    mock_transport._consume_payload = mock.AsyncMock(return_value=resp_data[0])
    headers = dict(key="value")

    with pytest.raises(TransportError, match=str(post_exception)):
        await mock_transport._send_final_payload(payload, headers=headers)

    expected_log_message = "Failed to send payload, {}".format(post_exception)
    assert expected_log_message in caplog.messages

    mock_transport._http_semaphore.__aenter__.assert_awaited()
    mock_transport._http_semaphore.__aexit__.assert_awaited()
    session.post.assert_awaited_with(
        mock_transport._url,
        json=payload,
        ssl=mock_transport.ssl,
        headers=headers,
        timeout=mock_transport.request_timeout,
    )
    mock_transport._consume_payload.assert_not_awaited()


async def test_send_payload_final_payload_missing_response(
    mock_transport, caplog
):
    caplog.set_level(logging.WARN)

    resp_data = [{"channel": "test/channel3", "data": {}, "id": 4}]
    response_mock = mock.MagicMock()
    response_mock.json = mock.AsyncMock(return_value=resp_data)
    response_mock.headers = object()
    session = mock.MagicMock()
    session.post = mock.AsyncMock(return_value=response_mock)
    mock_transport._http_session = session
    mock_transport._http_semaphore = mock.MagicMock()
    payload = [object(), object()]
    mock_transport.ssl = object()
    mock_transport._consume_payload = mock.AsyncMock(return_value=None)
    headers = dict(key="value")
    error_message = (
        "No response message received for the " "first message in the payload"
    )

    with pytest.raises(TransportError, match=error_message):
        await mock_transport._send_final_payload(payload, headers=headers)

    assert error_message in caplog.messages

    mock_transport._http_semaphore.__aenter__.assert_awaited()
    mock_transport._http_semaphore.__aexit__.assert_awaited()
    session.post.assert_awaited_with(
        mock_transport._url,
        json=payload,
        ssl=mock_transport.ssl,
        headers=headers,
        timeout=mock_transport.request_timeout,
    )
    response_mock.json.assert_awaited_with(loads=mock_transport._json_loads)
    mock_transport._consume_payload.assert_awaited_with(
        resp_data,
        headers=response_mock.headers,
        find_response_for=payload[0],
    )
