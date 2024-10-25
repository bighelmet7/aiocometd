import asyncio
from unittest import mock

import pytest

from aiocometd.utils import (
    get_error_message,
    get_error_code,
    get_error_args,
    defer,
    is_auth_error_message,
    is_event_message,
    is_server_error_message,
    is_matching_response,
)
from aiocometd.constants import MetaChannel, SERVICE_CHANNEL_PREFIX

# This is the same as using the @pytest.mark.anyio on all test functions in the module
pytestmark = pytest.mark.anyio


@pytest.fixture
def mock_coro_function():
    async def coro_func(value):
        return value

    return coro_func


@pytest.fixture
def mock_sleep():
    with mock.patch("aiocometd.utils.asyncio.sleep") as mock_func:
        yield mock_func


@pytest.fixture
def mock_get_error_code():
    with mock.patch("aiocometd.utils.get_error_code") as mock_func:
        yield mock_func


def test_get_error_code():
    error_field = "123::"
    result = get_error_code(error_field)

    assert result == 123


def test_get_error_code_none_field():
    error_field = None

    result = get_error_code(error_field)

    assert result is None


def test_get_error_code_empty_fielde():
    error_field = ""

    result = get_error_code(error_field)

    assert result is None


def test_get_error_code_invalid_field():
    error_field = "invalid"

    result = get_error_code(error_field)

    assert result is None


def test_get_error_code_short_invalid_field():
    error_field = "12::"

    result = get_error_code(error_field)

    assert result is None


def test_get_error_code_empty_code_field():
    error_field = "::"

    result = get_error_code(error_field)

    assert result is None


def test_get_error_message():
    error_field = "::message"

    result = get_error_message(error_field)

    assert result == "message"


def test_get_error_message_none_field():
    error_field = None

    result = get_error_message(error_field)

    assert result is None


def test_get_error_message_empty_field():
    error_field = ""

    result = get_error_message(error_field)

    assert result is None


def test_get_error_message_invalid_field():
    error_field = "invalid"

    result = get_error_message(error_field)

    assert result is None


def test_get_error_message_empty_code_field():
    error_field = "::"

    result = get_error_message(error_field)

    assert result == ""


def test_get_error_args():
    error_field = "403:xj3sjdsjdsjad,/foo/bar:Subscription denied"

    result = get_error_args(error_field)

    assert result == ["xj3sjdsjdsjad", "/foo/bar"]


def test_get_error_args_none_field():
    error_field = None

    result = get_error_args(error_field)

    assert result is None


def test_get_error_args_empty_field():
    error_field = ""

    result = get_error_args(error_field)

    assert result is None


def test_get_error_args_invalid_field():
    error_field = "invalid"

    result = get_error_args(error_field)

    assert result is None


def test_get_error_args_empty_code_field():
    error_field = "::"

    result = get_error_args(error_field)

    assert result == []


async def test_defer(mock_coro_function, mock_sleep):
    delay = 10
    wrapper = defer(mock_coro_function, delay, loop=None)

    argument = object()
    result = await wrapper(argument)

    assert result is argument
    mock_sleep.assert_called_with(delay)


async def test_defer_no_loop(mock_coro_function, mock_sleep):
    delay = 10
    wrapper = defer(mock_coro_function, delay)

    argument = object()
    result = await wrapper(argument)

    assert result is argument
    mock_sleep.assert_called_with(delay)


async def test_defer_none_delay(mock_coro_function, mock_sleep):
    wrapper = defer(mock_coro_function)

    argument = object()
    result = await wrapper(argument)

    assert result is argument
    mock_sleep.assert_not_called()


async def test_defer_zero_delay(mock_coro_function, mock_sleep):
    delay = 0
    wrapper = defer(mock_coro_function, delay)

    argument = object()
    result = await wrapper(argument)

    assert result is argument
    mock_sleep.assert_not_called()


async def test_defer_sleep_canceled(mock_coro_function, mock_sleep):
    delay = 10
    wrapper = defer(mock_coro_function, delay)
    mock_sleep.side_effect = asyncio.CancelledError()

    argument = object()
    with pytest.raises(asyncio.CancelledError):
        await wrapper(argument)

    mock_sleep.assert_called_with(delay)


def test_is_auth_error_message(mock_get_error_code):
    response = {"error": "error"}
    mock_get_error_code.return_value = 401

    result = is_auth_error_message(response)

    assert result == True
    mock_get_error_code.assert_called_with(response["error"])


def test_is_auth_error_message_forbidden(mock_get_error_code):
    response = {"error": "error"}
    mock_get_error_code.return_value = 403

    result = is_auth_error_message(response)

    assert result == True
    mock_get_error_code.assert_called_with(response["error"])


def test_is_auth_error_message_not_an_auth_error(mock_get_error_code):
    response = {"error": "error"}
    mock_get_error_code.return_value = 400

    result = is_auth_error_message(response)

    assert result == False
    mock_get_error_code.assert_called_with(response["error"])


def test_is_auth_error_message_not_an_error(mock_get_error_code):
    response = {}
    mock_get_error_code.return_value = None

    result = is_auth_error_message(response)

    assert result == False
    mock_get_error_code.assert_called_with(None)


def _assert_event_message_for_channel(
    channel, has_data, has_id, expected_result
):
    """
    Helper function for `is_event_message()`
    """
    message = dict(channel=channel)
    if has_data:
        message["data"] = None
    if has_id:
        message["id"] = None

    result = is_event_message(message)

    assert result == expected_result


def test_is_event_message_subscribe():
    channel = MetaChannel.SUBSCRIBE
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, False)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_is_event_message_unsubscribe():
    channel = MetaChannel.UNSUBSCRIBE
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, False)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_is_event_message_non_meta_channel():
    channel = "/test/channel"
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, True)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, True)


def test_is_event_message_service_channel():
    channel = SERVICE_CHANNEL_PREFIX + "test"
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, True)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_is_event_message_handshake():
    channel = MetaChannel.HANDSHAKE
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, False)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_is_event_message_connect():
    channel = MetaChannel.CONNECT
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, False)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_is_event_message_disconnect():
    channel = MetaChannel.DISCONNECT
    _assert_event_message_for_channel(channel, False, False, False)
    _assert_event_message_for_channel(channel, True, False, False)
    _assert_event_message_for_channel(channel, False, True, False)
    _assert_event_message_for_channel(channel, True, True, False)


def test_successful():
    message = {"successful": True}

    assert is_server_error_message(message) == False


def test_not_successful():
    message = {"successful": False}

    assert is_server_error_message(message) == True


def test_no_success_status():
    message = {}

    assert is_server_error_message(message) == False


def test_is_matching_response():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
        "id": "1",
    }
    response = {
        "channel": "/test/channel1",
        "successful": True,
        "clientId": "clientId",
        "id": "1",
    }

    assert is_matching_response(response, message) == True


def test_is_matching_response_response_none():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
        "id": "1",
    }
    response = None

    assert is_matching_response(response, message) == False


def test_is_matching_response_message_none():
    message = None
    response = {
        "channel": "/test/channel1",
        "successful": True,
        "clientId": "clientId",
        "id": "1",
    }

    assert is_matching_response(response, message) == False


def test_is_matching_response_without_id():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
    }
    response = {
        "channel": "/test/channel1",
        "successful": True,
        "clientId": "clientId",
    }

    assert is_matching_response(response, message) == True


def test_is_matching_response_different_id():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
        "id": "1",
    }
    response = {
        "channel": "/test/channel1",
        "successful": True,
        "clientId": "clientId",
        "id": "2",
    }

    assert is_matching_response(response, message) == False


def test_is_matching_response_different_channel():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
        "id": "1",
    }
    response = {
        "channel": "/test/channel2",
        "successful": True,
        "clientId": "clientId",
        "id": "1",
    }

    assert is_matching_response(response, message) == False


def test_is_matching_response_without_successful_field():
    message = {
        "channel": "/test/channel1",
        "data": {},
        "clientId": "clientId",
        "id": "1",
    }
    response = {
        "channel": "/test/channel1",
        "clientId": "clientId",
        "id": "1",
    }

    assert is_matching_response(response, message) == False
