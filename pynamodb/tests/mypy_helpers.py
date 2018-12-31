import re
import tempfile
from collections import defaultdict
from textwrap import dedent
from typing import Dict, List, Iterable

import pytest

mypy_api = pytest.importorskip("mypy.api")


def _run_mypy(program):  # type: (str) -> Iterable[str]
    with tempfile.TemporaryDirectory() as tempdirname:
        with open('{}/__main__.py'.format(tempdirname), 'w') as f:
            f.write(program)
        error_pattern = re.compile(r'^{}:(\d+): error: (.*)$'.format(re.escape(f.name)))
        stdout, stderr, exit_status = mypy_api.run([f.name])

        # Group errors by line
        errors_by_line: Dict[int, List[str]] = defaultdict(list)
        for line in stdout.split('\n'):
            m = error_pattern.match(line)
            if m:
                errors_by_line[int(m.group(1))].append(m.group(2))

        # Reconstruct the "actual" program with "error" comments
        pattern_optional_comment = re.compile(r'^(.*?)(\s+# E: .*)?$')
        for line_no, line in enumerate(program.split('\n'), start=1):
            m = pattern_optional_comment.match(line)
            errors = errors_by_line.get(line_no)
            if errors:
                yield '{}{}'.format(m.group(1), ''.join('  # E: {}'.format(error) for error in errors))
            else:
                yield m.group(1)


def assert_mypy_output(program):  # type: (str) -> None
    program = dedent(program).strip()
    actual = '\n'.join(_run_mypy(program))
    assert actual == program
