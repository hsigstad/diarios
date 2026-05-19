# CNJ SGT reference tables

Three CSVs in this directory mirror CNJ's *Sistema de Gestão de Tabelas*
(SGT) — the official Brazilian-judiciary taxonomy underlying the
Tabela Processual Unificada (TPU):

| File              | tipo_item | rows   | what                       |
|-------------------|-----------|--------|----------------------------|
| `cnj_movs.csv`    | `M`       |    964 | procedural movement codes  |
| `cnj_classes.csv` | `C`       |    849 | case classes (e.g. 64=ACIA)|
| `cnj_assuntos.csv`| `A`       |  5,601 | subject-matter codes       |

## Schema (per row)

| column                 | meaning                                           |
|------------------------|---------------------------------------------------|
| `cod_item`             | canonical CNJ integer code                        |
| `cod_item_pai`         | parent code in the hierarchy (NaN at root)        |
| `nome`                 | official Portuguese name                          |
| `situacao`             | `A` (active) or `I` (inactive)                    |
| `dat_versao`           | SGT version timestamp                             |
| `dsc_caminho_completo` | full hierarchical path string                     |

## Provenance

Source: CNJ SGT `dump_dados.sql`, version 78, dated 2025-09-11.
Downloaded from `https://www.cnj.jus.br/sgt/enviarArquivo.php?url=78_dump_dados.sql&nome=dump_dados.sql`
on 2026-05-19. The dump is a MySQL dump of the `sgt_consulta.itens`
table; rows were filtered by `tipo_item` ∈ {M, C, A} and a slim
column subset retained.

To refresh, re-download the dump and re-run the parser at
`packages/diarios/scripts/refresh_cnj_tables.py` (or repeat the
ad-hoc script that produced this set — see the diarios commit log
for the original procedure).

## Usage

```python
from diarios.clean import load_cnj_table, cnj_label

movs = load_cnj_table("mov")          # DataFrame
cnj_label(219)                         # 'Procedência'
cnj_label(64, kind="classe")           # 'Ação Civil de Improbidade Administrativa'
cnj_label(1116, kind="classe")         # 'Execução Fiscal'
```

## Spot-check (merit-judgment codes)

| code  | nome                                              |
|-------|---------------------------------------------------|
| 219   | Procedência                                       |
| 220   | Improcedência                                     |
| 221   | Procedência em Parte                              |
| 196   | Extinção da execução ou do cumprimento da sentença|
| 466   | Homologação de Transação                          |
| 471   | Pronúncia de Decadência ou Prescrição             |
| 454   | Indeferimento da petição inicial                  |
| 848   | Trânsito em julgado                               |
| 22    | Baixa Definitiva                                  |
| 12164 | Outras Decisões (unclassified bucket)             |
