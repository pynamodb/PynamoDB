import re
from collections import namedtuple
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest

mypy_api = pytest.importorskip("mypy.api")


_MypyError = namedtuple('_MypyError', 'line_no error')


def _run_mypy(program):
    with TemporaryDirectory() as tempdirname:
        with open('{}/__main__.py'.format(tempdirname), 'w') as f:
            f.write(dedent(program))
        error_pattern = re.compile(r'^{}:(\d+): error: (.*)$'.format(re.escape(f.name)))
        stdout, stderr, exit_status = mypy_api.run([f.name])
        for line in stdout.split('\n'):
            m = error_pattern.match(line)
            if m:
                yield _MypyError(line_no=int(m.group(1)), error=m.group(2))


def _parse_expected_mypy_errors(program):
    error_pattern = re.compile(r'# E: (.+?)(?=\s*#|$)')  # matches every error expected on a given line
    for line_no, line in enumerate(program.split('\n'), start=1):
        for error in error_pattern.findall(line):
            yield _MypyError(line_no=line_no, error=error)


def _repr_mypy_errors(errors):
    return '\n'.join('Line {}: {}'.format(error.line_no, error.error) for error in errors)


def assert_mypy_output(program):
    actual = _run_mypy(program)
    expected = _parse_expected_mypy_errors(program)
    assert _repr_mypy_errors(actual) == _repr_mypy_errors(expected)
