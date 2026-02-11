"""Read and concatenate vara (court division) CSV files."""

from glob import glob

import pandas as pd
import path

# TODO: Fix when extracting new data (with characters instead of numbers in, e.g., vara column)

def read_csv(infile: str) -> pd.DataFrame:
    """Read a single vara CSV file with latin1 encoding.

    Args:
        infile: Path to the CSV file.

    Returns:
        DataFrame from the file.
    """
    return pd.read_csv(infile, encoding='latin1')


if __name__ == '__main__':
    infiles = glob('{}/varas/vara*.csv'.format(path.db_dir))
    df = pd.concat(map(read_csv, infiles))

    #df.to_csv('diarios/data/vara.csv', index=False)
