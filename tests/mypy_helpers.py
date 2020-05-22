import os
import re
import shutil
import sys
from collections import defaultdict
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple


def _run_mypy(program: str, *, use_pdb: bool) -> Iterable[str]:
    import mypy.api

    with TemporaryDirectory() as tempdirname:
        with open('{}/__main__.py'.format(tempdirname), 'w') as f:
            f.write(program)
        error_pattern = re.compile(fr'^{re.escape(f.name)}:'
                                   r'(?P<line>\d+): (?P<level>note|warning|error): (?P<message>.*)$')
        mypy_args = [
            f.name,
            '--show-traceback',
            '--raise-exceptions',
            '--show-error-codes',
        ]
        src_config_file_path = os.path.dirname(__file__) + '/mypy.ini'
        if os.path.exists(src_config_file_path):
            dest_config_file_path = tempdirname + '/mypy.ini'
            shutil.copyfile(src_config_file_path, dest_config_file_path)
            mypy_args += ['--config-file', dest_config_file_path]
        if use_pdb:
            mypy_args += ['--pdb']
        stdout, stderr, exit_status = mypy.api.run(mypy_args)
        if stderr:
            print(stderr, file=sys.stderr)  # allow "printf debugging" of the plugin

        # Group errors by line
        messages_by_line: Dict[int, List[Tuple[str, str]]] = defaultdict(list)
        for line in stdout.split('\n'):
            m = error_pattern.match(line)
            if m:
                messages_by_line[int(m.group('line'))].append((m.group('level'), m.group('message')))
            elif line:
                # print(line)  # allow "printf debugging"
                pass

        # Reconstruct the "actual" program with "error" comments
        error_comment_pattern = re.compile(r'(\s+# (N|W|E): .*)?$')
        for line_no, line in enumerate(program.split('\n'), start=1):
            line = error_comment_pattern.sub('', line)
            messages = messages_by_line.get(line_no)
            if messages:
                messages_str = ''.join(f'  # {level[0].upper()}: {message}' for level, message in messages)
                yield f'{line}{messages_str}'
            else:
                yield line


def assert_mypy_output(program: str, *, use_pdb: bool) -> None:
    expected = dedent(program).strip()
    actual = '\n'.join(_run_mypy(expected, use_pdb=use_pdb))
    assert actual == expected
