import re
import sys
from collections import defaultdict
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Dict
from typing import Iterable
from typing import List

import mypy.api


def _run_mypy(program: str) -> Iterable[str]:
    with TemporaryDirectory() as tempdirname:
        with open(f'{tempdirname}/__main__.py', 'w') as f:
            f.write(program)
        error_pattern = re.compile(fr'^{re.escape(f.name)}:(\d+): error: (.*)$')
        stdout, stderr, exit_status = mypy.api.run([
            f.name,
            '--show-traceback',
        ])
        if stderr:
            print(stderr, file=sys.stderr)  # allow "printf debugging" of the plugin

        # Group errors by line
        errors_by_line: Dict[int, List[str]] = defaultdict(list)
        for line in stdout.split('\n'):
            m = error_pattern.match(line)
            if m:
                errors_by_line[int(m.group(1))].append(m.group(2))
            elif line:
                print('x', repr(line))  # allow "printf debugging" of the plugin

        # Reconstruct the "actual" program with "error" comments
        error_comment_pattern = re.compile(r'(\s+# E: .*)?$')
        for line_no, line in enumerate(program.split('\n'), start=1):
            line = error_comment_pattern.sub('', line)
            errors = errors_by_line.get(line_no)
            if errors:
                yield line + ''.join(f'  # E: {error}' for error in errors)
            else:
                yield line


def assert_mypy_output(program: str) -> None:
    program = dedent(program).strip()
    actual = '\n'.join(_run_mypy(program))
    assert actual == program
