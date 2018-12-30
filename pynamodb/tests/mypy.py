import re
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import NamedTuple, Iterable

import mypy.api


class _MypyError(NamedTuple):
    line_no: int
    error: str

    def __repr__(self) -> str:
        return f'Line {self.line_no}: {self.error}'


def run_mypy(program: str) -> Iterable[_MypyError]:
    with TemporaryDirectory() as tempdirname:
        with open(f'{tempdirname}/__main__.py', 'w') as f:
            f.write(dedent(program))
        error_pattern = re.compile(rf'^{re.escape(f.name)}:(\d+): error: (.*)$')
        for line in mypy.api.run([f.name, '-v'])[0].split('\n'):
            m = error_pattern.match(line)
            if m:
                yield _MypyError(line_no=int(m.group(1)), error=m.group(2))


def parse_expected_mypy_errors(program: str) -> Iterable[_MypyError]:
    error_pattern = re.compile(rf'# E: (.+?)(?=\s*#|$)')  # matches every error expected on a given line
    for line_no, line in enumerate(program.split('\n'), start=1):
        for error in error_pattern.findall(line):
            yield _MypyError(line_no=line_no, error=error)
