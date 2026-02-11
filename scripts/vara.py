import path
import pandas as pd
import numpy as np
import os
from diarios.clean import transform
from glob import glob

# TODO: Fix when extracting new data (with characters instead of numbers in, e.g., vara column)

infiles = glob('{}/varas/vara*.csv'.format(path.db_dir))

def read_csv(infile):
    return pd.read_csv(infile, encoding='latin1')

df = pd.concat(map(read_csv, infiles))

#df.to_csv('diarios/data/vara.csv', index=False)
