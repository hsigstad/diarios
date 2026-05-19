"""Refresh diarios/data/cnj_{movs,classes,assuntos}.csv from CNJ SGT.

INTENT: re-derive the canonical CNJ TPU reference tables from CNJ's
``Sistema de Gestão de Tabelas``. Run when CNJ publishes a new version
(announced at https://www.cnj.jus.br/sgt/versoes.php?tipo_tabela=M).

REASONING: the SGT exposes the full taxonomy as a MySQL dump at
``dump_dados.sql``. We parse the INSERT statements for the
``sgt_consulta.itens`` table, filter by ``tipo_item`` (M/C/A), and
write three slim CSVs into the package's ``data/`` directory.

ASSUMES: network access to www.cnj.jus.br. The dump is encoded in
latin1 (Portuguese characters are NOT utf-8 in the raw dump).

Usage:
    python scripts/refresh_cnj_tables.py
"""

from __future__ import annotations

import csv
import re
import urllib.request
from pathlib import Path

SGT_URL = (
    "https://www.cnj.jus.br/sgt/enviarArquivo.php"
    "?url=78_dump_dados.sql&nome=dump_dados.sql"
)
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
PKG_DATA = Path(__file__).resolve().parent.parent / "diarios" / "data"

VALUE_RE = re.compile(r"NULL|'((?:[^']|'')*)'", re.DOTALL)
COLUMNS = [
    "cod_item", "cod_item_pai", "tipo_item", "nome", "situacao",
    "dat_inclusao", "usu_inclusao", "dat_alteracao", "usu_alteracao",
    "dat_versao", "num_versao_lancado", "dat_inativacao", "dat_reativacao",
    "dat_inicio_vigencia", "dat_fim_vigencia",
    "tip_hierarquia_item", "dsc_caminho_completo",
]
KEEP = ["cod_item", "cod_item_pai", "nome", "situacao", "dat_versao", "dsc_caminho_completo"]
TYPE_TO_FILE = {"M": "cnj_movs.csv", "C": "cnj_classes.csv", "A": "cnj_assuntos.csv"}


def parse_values(s: str) -> list[str | None]:
    out: list[str | None] = []
    for m in VALUE_RE.finditer(s):
        out.append(None if m.group(0) == "NULL" else m.group(1).replace("''", "'"))
    return out


def fetch_dump(dest: Path) -> None:
    req = urllib.request.Request(SGT_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp, dest.open("wb") as fh:
        while chunk := resp.read(65536):
            fh.write(chunk)


def parse_dump(dump_path: Path) -> dict[str, list[dict]]:
    by_type: dict[str, list[dict]] = {"M": [], "C": [], "A": []}
    with dump_path.open(encoding="latin1") as fh:
        for line in fh:
            if not line.startswith("INSERT INTO sgt_consulta.itens"):
                continue
            i = line.index("(", line.rfind("VALUES"))
            j = line.rfind(")")
            values = parse_values(line[i:j])
            if len(values) != len(COLUMNS):
                continue
            row = dict(zip(COLUMNS, values))
            if row["tipo_item"] in by_type:
                by_type[row["tipo_item"]].append(row)
    return by_type


def write_csvs(by_type: dict[str, list[dict]], outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    for t, rows in by_type.items():
        path = outdir / TYPE_TO_FILE[t]
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=KEEP, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"{path}: {len(rows):,} rows")


def main() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        dump = Path(td) / "dump.sql"
        print(f"fetching {SGT_URL} ...")
        fetch_dump(dump)
        print(f"  {dump.stat().st_size / 1e6:.1f} MB")
        by_type = parse_dump(dump)
        write_csvs(by_type, PKG_DATA)
    print("done.")


if __name__ == "__main__":
    main()
