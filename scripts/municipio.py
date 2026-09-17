"""Refresh diarios/data/municipio.csv from the territorio pipeline.

The município dimension is now BUILT in `pipelines/territorio`
(`source/clean/municipio.py` → `build/clean/municipio.parquet`). diarios no
longer derives it — this script only re-exports territorio's canonical table to
the bundled CSV that diarios' runtime geo functions (and every court-data
consumer) read. Schema is unchanged, so consumers are unaffected.

USAGE (run where the territorio build exists):
    python scripts/municipio.py
    # override the source path:
    TERRITORIO_MUNICIPIO=/path/to/municipio.parquet python scripts/municipio.py

After running, verify `git diff diarios/data/municipio.csv` is EMPTY before
committing — a non-empty diff means the territorio port diverged from the old
builder and must be reconciled, not committed.
"""
import os

import pandas as pd

# Column order must stay identical to the historical municipio.csv.
COLS = [
    "municipio_id", "municipio", "municipio_accents", "ibge7", "ibge6",
    "estado", "estado_id", "comarca_id", "subsecao_id",
]

_HERE = os.path.dirname(os.path.abspath(__file__))
# packages/diarios/scripts -> workspace root (../../..) -> pipelines/territorio
_DEFAULT_SRC = os.path.normpath(os.path.join(
    _HERE, "..", "..", "..",
    "pipelines", "territorio", "build", "clean", "municipio.parquet"))
_OUT = os.path.join(_HERE, "..", "diarios", "data", "municipio.csv")


def main() -> pd.DataFrame:
    src = os.environ.get("TERRITORIO_MUNICIPIO", _DEFAULT_SRC)
    if not os.path.exists(src):
        raise FileNotFoundError(
            f"territorio município build not found at {src}. Build it first: "
            "`python source/clean/municipio.py` in pipelines/territorio "
            "(needs the raw inputs + an installed diarios), or set "
            "$TERRITORIO_MUNICIPIO."
        )
    df = pd.read_parquet(src)[COLS]
    df.to_csv(_OUT, index=False)
    print(f"refreshed {os.path.normpath(_OUT)}: {len(df):,} rows from {src}")
    return df


if __name__ == "__main__":
    main()
