import subprocess
import os
import glob

class Extractor:
    
    def __init__(self, inpath, outpath):
        self.inpath = inpath
        self.outpath = outpath

    def extract(self, regex, infiles, outfile):
        os.chdir(self.inpath)
        files = glob.glob(infiles)
        cmd = [
            'pcre2grep', '-HMon', regex
        ] + files
        outfile = os.path.join(self.outpath, outfile)
        with open(outfile, 'w') as f:
            subprocess.call(cmd, stdout=f)

    def extract_sections(
            self, start, end,
            infiles, outfile,            
            max_length=10000
        ):
        regex = '(?s){0}.{{1,{1}}}?(?={2})'.format(
            start, max_length, end
        )
        self.extract(regex, infiles, outfile)
