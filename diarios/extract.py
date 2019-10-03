import subprocess
import os
import glob

class Extractor:
    
    def __init__(self, inpath, outpath):
        self.inpath = inpath
        self.outpath = outpath

    def extract(self, regex, infiles, outfile):
        os.chdir(self.inpath)
        cmd = [
            'pcre2grep', '-HMon', regex
        ]
        outfile = os.path.join(self.outpath, outfile)
        try:
            os.remove(outfile)
        except FileNotFoundError:
            pass
        with open(outfile, 'a') as f:
            for infile in glob.glob(infiles):
                cmd2 = cmd + [infile]
                subprocess.call(cmd2, stdout=f)

    def extract_sections(
            self, start, end,
            infiles, outfile,            
            max_length=10000
        ):
        # Ideally this should return a max_length
        # string if end is not found. Currently it
        # returns no match in this case. Not sure
        # how to fix this.
        regex = '(?s){0}.{{1,{1}}}?(?={2})'.format(
            start, max_length, end
        )
        self.extract(regex, infiles, outfile)
