import subprocess
import os
from glob import glob


class Extractor:
    def __init__(self, inpath, outpath):
        self.inpath = inpath
        self.outpath = outpath

    def extract(self,
                regex,
                infiles,
                outfile,
                cmd=None,
                post=None,
                header=None,
                append=False):
        start_dir = os.getcwd()
        os.chdir(self.inpath)
        if not cmd:
            cmd = ['pcre2grep', '-HMon']
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

    def extract_sections(self,
                         start,
                         end,
                         infiles,
                         outfile,
                         max_length=10000,
                         **kwargs):
        # Ideally this should return a max_length
        # string if end is not found. Currently it
        # returns no match in this case. Not sure
        # how to fix this.
        regex = '(?s){0}.{{1,{1}}}?(?={2})'.format(start, max_length, end)
        self.extract(regex, infiles, outfile, **kwargs)


def _run_cmd(cmd, post, infile, f):
    cmd2 = cmd + [infile]
    #ps = subprocess.Popen(cmd2, stdout=f)
    if post:
        ps = subprocess.Popen(cmd2, stdout=subprocess.PIPE)
        for p in post[:-1]:
            ps = subprocess.Popen(p, stdin=ps.stdout, stdout=subprocess.PIPE)
        subprocess.call(post[-1], stdin=ps.stdout, stdout=f)
    else:
        subprocess.call(cmd2, stdout=f)
