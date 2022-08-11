import pytest
from requests import Response

from cdisc_rules_engine.exceptions.custom_exceptions import NumberOfAttemptsExceeded
from cdisc_rules_engine.utilities.decorators import retry_request


def test_retry_request_decorator():
    """
    Unit test for retry_request.
    Testing the case when a wrapped function
    returned a response that should not be retried.
    """
    response_to_return = Response()
    response_to_return.status_code = 200

    @retry_request(
        5,
        [
            500,
        ],
    )
    def wrapped_func():
        return response_to_return

    response = wrapped_func()
    assert response == response_to_return


def test_retry_request_decorator_attempts_exceeded():
    """
    Unit test for retry_request.
    Testing the case when a wrapped function
    returned a response that should be retried
    and the number of attempts exceeded.
    """

    @retry_request(
        5,
        [
            500,
        ],
    )
    def wrapped_func():
        response = Response()
        response.status_code = 500
        return response

    with pytest.raises(NumberOfAttemptsExceeded):
        wrapped_func()
