import pytest


@pytest.fixture
def assert_mypy_output(pytestconfig):
    pytest.importorskip('mypy')  # we only install mypy in python>=3.6 tests
    pytest.register_assert_rewrite('tests.mypy_helpers')

    from tests.mypy_helpers import assert_mypy_output
    return lambda program: assert_mypy_output(program, use_pdb=pytestconfig.getoption('usepdb'))
