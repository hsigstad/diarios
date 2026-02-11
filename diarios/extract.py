"""Regex-based text extraction from files using pcre2grep."""

from __future__ import annotations

from typing import Any, List, Optional

import subprocess
import os
from glob import glob

__all__ = [
    "Extractor",
]


class Extractor:
    """Extract text matching regex patterns from files using pcre2grep.

    Args:
        inpath: Directory containing input files.
        outpath: Directory for output files.
    """

    def __init__(self, inpath: str, outpath: str) -> None:
        self.inpath = inpath
        self.outpath = outpath

    def extract(
        self,
        regex: str,
        infiles: str,
        outfile: str,
        cmd: Optional[List[str]] = None,
        post: Optional[List[List[str]]] = None,
        header: Optional[str] = None,
        append: bool = False,
    ) -> None:
        """Extract text matching a regex from input files.

        Args:
            regex: PCRE2 regex pattern to match.
            infiles: Glob pattern for input files (relative to ``inpath``).
            outfile: Output filename (written inside ``outpath``).
            cmd: Custom command list; defaults to pcre2grep with standard flags.
            post: Optional list of piped post-processing commands.
            header: Optional header string written at the top of the output file.
            append: If True, append to the output file instead of overwriting.
        """
        start_dir = os.getcwd()
        os.chdir(self.inpath)
        if not cmd:
            cmd = [
                'pcre2grep', '-HMon',
                '--buffer-size=100000000', #300MB? (larger gives malloc failed)
                '--max-buffer-size=100000000', #3GBex
            ]
        cmd.append(regex)
        outfile = os.path.join(self.outpath, outfile)
        if append:
            mode = 'a'
        else:
            mode = 'w'
        with open(outfile, mode) as f:
            if header:
                f.write(header)
                f.flush()
            for infile in glob(infiles):
                _run_cmd(cmd, post, infile, f)
        os.chdir(start_dir)

    def extract_sections(
        self,
        start: str,
        end: str,
        infiles: str,
        outfile: str,
        max_length: int = 10000,
        **kwargs: Any,
    ) -> None:
        """Extract text sections delimited by start and end patterns.

        Args:
            start: Regex marking the beginning of a section.
            end: Regex marking the end (used as a lookahead).
            infiles: Glob pattern for input files.
            outfile: Output filename.
            max_length: Maximum characters between start and end.
            **kwargs: Extra arguments forwarded to ``extract``.
        """
        # Ideally this should return a max_length
        # string if end is not found. Currently it
        # returns no match in this case. Not sure
        # how to fix this.
        regex = '(?s){0}.{{1,{1}}}?(?={2})'.format(start, max_length, end)
        self.extract(regex, infiles, outfile, **kwargs)


def _run_cmd(
    cmd: List[str],
    post: Optional[List[List[str]]],
    infile: str,
    f: Any,
) -> None:
    """Run a shell command pipeline, writing output to a file handle.

    Args:
        cmd: Base command list.
        post: Optional list of piped post-processing commands.
        infile: Input file to append to the command.
        f: Open file handle for writing output.
    """
    cmd2 = cmd + [infile]
    #ps = subprocess.Popen(cmd2, stdout=f)
    if post:
        ps = subprocess.Popen(cmd2, stdout=subprocess.PIPE)
        for p in post[:-1]:
            ps = subprocess.Popen(p, stdin=ps.stdout, stdout=subprocess.PIPE)
        subprocess.call(post[-1], stdin=ps.stdout, stdout=f)
    else:
        subprocess.call(cmd2, stdout=f)
