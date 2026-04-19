# diarios

A Python package to extract information from Brazilian official diaries
(`diários da justiça`) and related court data sources. Provides parsers,
text-cleaning utilities, anonymization, geographic / case-number lookups,
and consulta clients for federal and state courts (STF, STJ, TJSP, TRF1).

## Requirements

- Python >= 3.9
- Dependencies are managed via `pyproject.toml`
- [pcre2grep](https://www.pcre.org/) (for `extract.py`)

## Installation

To install directly from GitHub:

```bash
pip install git+ssh://git@github.com/hsigstad/diarios.git
```

Or clone the repo and install in editable mode:

```bash
pip install -e ~/diarios
```

## Status

Research tooling — used across my own projects in empirical legal /
political economics. Not packaged for general use; APIs may change as
the projects evolve.

## Related repos

Part of a set of repositories I use across my research projects:

- [research-kit](https://github.com/hsigstad/research-kit) — Claude Code
  skills, conventions, methodology docs, tools
- [llmkit](https://github.com/hsigstad/llmkit) — LLM extraction toolkit
  with caching and audit
- [newsbr](https://github.com/hsigstad/newsbr) — Brazilian news collection
- [brazil-institutions](https://github.com/hsigstad/brazil-institutions) —
  institutional reference for Brazil-focused research
