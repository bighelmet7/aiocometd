from unittest import mock

import pytest

from aiocometd.exceptions import ServerError


@pytest.fixture
def mock_utils():
    with mock.patch("aiocometd.exceptions.utils") as mock_func:
        yield mock_func


def test_properties():
    response = {
        "channel": "/meta/subscription",
        "successful": False,
        "id": "0",
        "error": "error",
    }

    error = ServerError("description message", response)

    assert error.message == "description message"
    assert error.response == response
    assert error.error == "error"


def test_properties_on_no_error():
    response = {
        "channel": "/meta/subscription",
        "successful": False,
        "id": "0",
    }

    error = ServerError("description message", response)

    assert error.message == "description message"
    assert error.response == response
    assert error.error is None


def test_properties_on_no_response():
    response = None

    error = ServerError("description message", response)

    assert error.message == "description message"
    assert error.response is None
    assert error.error is None


def test_error_code(mock_utils):
    response = {
        "channel": "/meta/subscription",
        "successful": False,
        "id": "0",
        "error": "error",
    }
    mock_utils.get_error_code.return_value = 12

    error = ServerError("description message", response)
    result = error.error_code

    assert result == mock_utils.get_error_code.return_value
    mock_utils.get_error_code.assert_called_with(response["error"])


def test_error_message(mock_utils):
    response = {
        "channel": "/meta/subscription",
        "successful": False,
        "id": "0",
        "error": "error",
    }
    mock_utils.get_error_message.return_value = "message"

    error = ServerError("description message", response)
    result = error.error_message

    assert result == mock_utils.get_error_message.return_value
    mock_utils.get_error_message.assert_called_with(response["error"])


def test_error_args(mock_utils):
    response = {
        "channel": "/meta/subscription",
        "successful": False,
        "id": "0",
        "error": "error",
    }
    mock_utils.get_error_args.return_value = ["arg"]

    error = ServerError("description message", response)
    result = error.error_args

    assert result == mock_utils.get_error_args.return_value
    mock_utils.get_error_args.assert_called_with(response["error"])
