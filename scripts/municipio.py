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

After running, review `git diff diarios/data/municipio.csv` before committing. It
should show ONLY intended changes: as of 2026-09-23 that is the 169 `comarca_id`
rows where territorio adopted the reconciled resolution ledger (see
pipelines/territorio docs/reference/comarca_ipea.md). Any change to another column,
or to more rows, means the territorio port diverged and must be reconciled, not
committed.
"""
import os

import pandas as pd

# Column order — identity only. comarca_id/subsecao_id were dropped 2026-09-23
# (judiciary org moved to justica; resolve via diarios get_comarca_id/get_subsecao_id,
# which read justica's municipio__comarca.csv / the year-aware panel).
COLS = [
    "municipio_id", "municipio", "municipio_accents", "ibge7", "ibge6",
    "estado", "estado_id",
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
