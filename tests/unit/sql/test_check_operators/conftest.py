import pytest

from . import helpers


@pytest.fixture(autouse=True, params=[False, True], ids=["rows", "compiled"])
def compile_mode(request):
    """Every operator test runs against the row-by-row path and against the compiled path."""
    helpers.COMPILE_MODE = request.param
    yield request.param
    helpers.COMPILE_MODE = False
