#!/usr/bin/env python3
"""
Anonymization for Brazilian court text data.

Uses GLiNER NER to identify and replace person, judge, organization, and
location names. Uses regex to replace Brazilian identity numbers (CPF, RG,
CNPJ, PIS, CTPS, OAB, CNJ process numbers).

Heavy dependencies (`gliner`, `transformers`, `nltk`) are NOT installed by
default. Install them via the optional extra:

    pip install diarios[anonymize]

Usage as a module::

    from diarios.anonymize import anonymize_text, anonymize_dataframe
    out = anonymize_text("Anderson José Costa, CPF 296.003.258-63 ...")

Usage as a CLI::

    python -m diarios.anonymize -i raw_dir -o anon_dir
    python -m diarios.anonymize -i data.csv -o data_anon.csv --columns text notes

Model cache
-----------
Set ``DIARIOS_HF_CACHE_DIR`` to point at a directory containing pre-downloaded
HuggingFace models (subdirectories ``gliner_multi-v2.1`` and
``bert-base-multilingual-cased``). If unset or missing, the model is fetched
from HuggingFace on first use.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# Model identifiers
MODEL_HF = "urchade/gliner_multi-v2.1"
TOKENIZER_HF = "bert-base-multilingual-cased"

# Entity labels for NER
LABELS = ["person", "judge", "organization", "location"]

# Maximum tokens per chunk for NER processing
MAX_TOKENS = 384

# Lazy-loaded singletons
_model = None
_tokenizer = None


def _cache_paths() -> Tuple[Optional[Path], Optional[Path]]:
    """Resolve optional model/tokenizer cache paths from the environment."""
    cache_root = os.environ.get("DIARIOS_HF_CACHE_DIR")
    if not cache_root:
        return None, None
    root = Path(cache_root)
    model_dir = root / "gliner_multi-v2.1"
    tokenizer_dir = root / "bert-base-multilingual-cased"
    return (
        model_dir if model_dir.exists() else None,
        tokenizer_dir if tokenizer_dir.exists() else None,
    )


def _load_model():
    """Lazy load the GLiNER model and tokenizer."""
    global _model, _tokenizer

    if _model is not None and _tokenizer is not None:
        return _model, _tokenizer

    try:
        from gliner import GLiNER
        from transformers import AutoTokenizer
    except ImportError as e:
        raise ImportError(
            "diarios.anonymize requires the 'anonymize' extra. "
            "Install with: pip install diarios[anonymize]"
        ) from e

    model_cache, tokenizer_cache = _cache_paths()

    if _model is None:
        if model_cache is not None:
            _model = GLiNER.from_pretrained(str(model_cache))
        else:
            print(f"Downloading model from HuggingFace: {MODEL_HF}")
            _model = GLiNER.from_pretrained(MODEL_HF)

    if _tokenizer is None:
        if tokenizer_cache is not None:
            _tokenizer = AutoTokenizer.from_pretrained(str(tokenizer_cache))
        else:
            _tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_HF)

    return _model, _tokenizer


def _chunk_text(text: str, max_tokens: int = MAX_TOKENS) -> List[str]:
    """Split text into chunks that fit within the token limit.

    Uses sentence tokenization to preserve sentence boundaries.
    """
    from nltk.tokenize import sent_tokenize

    _, tokenizer = _load_model()

    try:
        sentences = sent_tokenize(text, language="portuguese")
    except LookupError:
        sentences = sent_tokenize(text)

    chunks: List[str] = []
    current_chunk = ""

    for sentence in sentences:
        test_chunk = current_chunk + " " + sentence if current_chunk else sentence
        tokens = tokenizer.tokenize(test_chunk)

        if len(tokens) <= max_tokens:
            current_chunk = test_chunk
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            if len(tokenizer.tokenize(sentence)) > max_tokens:
                chunks.append(sentence[: max_tokens * 4])
            else:
                current_chunk = sentence

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks if chunks else [text]


def _anonymize_chunk(chunk: str) -> str:
    """Anonymize a single chunk of text using NER."""
    model, _ = _load_model()

    try:
        entities = model.predict_entities(chunk, labels=LABELS)
    except Exception:
        return chunk

    for entity in sorted(entities, key=lambda x: -x["start"]):
        replacement = f"[{entity['label'].upper()}]"
        chunk = chunk[: entity["start"]] + replacement + chunk[entity["end"]:]

    return chunk


def replace_identity_numbers(text: str) -> str:
    """Replace Brazilian identity numbers with [IdentityNumber] placeholder.

    Handles: CPF, RG, CNPJ, PIS, CTPS, CIRG, OAB, CNJ process numbers,
    and generic "número" references.
    """
    patterns = [
        r'\b(?:CPF|C\.P\.F\.?)[^\d]{0,15}[\d./-]{8,20}',
        r'\b(?:RG|R\.G\.?|CIRG)[^\d]{0,15}[\d./-]{5,20}',
        r'\b(?:CNPJ|C\.N\.P\.J\.?)[^\d]{0,15}[\d./-]{10,25}',
        r'\b(?:PIS|PASEP|NIT)[^\d]{0,15}[\d./-]{8,20}',
        r'\b(?:CTPS|C\.T\.P\.S\.?)[^\d]{0,15}[\d./-]{5,20}',
        r'\b[Ii]dentidade\s+n[°º.]?\s*[\d./-]{5,20}',
        r'\bsob\s+(?:o\s+)?n[°º.]?\s*[\d./-]{5,20}',
        r'\bOAB[^\d]{0,10}[\d./-]{3,15}',
        r'\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b',
    ]

    for pattern in patterns:
        text = re.sub(pattern, '[IdentityNumber]', text, flags=re.IGNORECASE)

    return text


def anonymize_text(text: str, verbose: bool = False) -> str:
    """Anonymize a single text string.

    Args:
        text: The text to anonymize.
        verbose: If True, print progress.

    Returns:
        Anonymized text with entities replaced by placeholders.
    """
    if not text or not isinstance(text, str) or text.strip() == "":
        return text

    text = replace_identity_numbers(text)
    chunks = _chunk_text(text)
    anonymized_chunks = [_anonymize_chunk(chunk) for chunk in chunks]
    return " ".join(anonymized_chunks)


def anonymize_dataframe(
    df: pd.DataFrame,
    text_cols: List[str],
    verbose: bool = True,
    progress_interval: int = 100,
) -> pd.DataFrame:
    """Anonymize specified text columns in a DataFrame.

    Args:
        df: DataFrame to anonymize.
        text_cols: List of column names containing text to anonymize.
        verbose: If True, print progress updates.
        progress_interval: How often to print progress (every N rows).

    Returns:
        DataFrame with anonymized text columns.
    """
    df = df.copy()
    total = len(df)

    for col in text_cols:
        if col not in df.columns:
            continue

        if verbose:
            print(f"Anonymizing column: {col}")

        processed = [0]

        def _anonymize_with_progress(text):
            processed[0] += 1
            if verbose and processed[0] % progress_interval == 0:
                print(f"  Processed {processed[0]}/{total} rows...")
            return anonymize_text(text)

        df[col] = df[col].fillna("").apply(_anonymize_with_progress)

        if verbose:
            print(f"  Done: {processed[0]} rows anonymized")

    return df


# ---------------------------------------------------------------------
# Post-parsing sanitization for structured CSV columns
# ---------------------------------------------------------------------

# Values that are already anonymized (exact match)
_ANON_TOKEN_RE = re.compile(
    r'^\[(?:PERSON|JUDGE|ORGANIZATION|LOCATION|IdentityNumber)\]$'
)

# Values that contain an anonymization token (partially anonymized)
_CONTAINS_ANON_RE = re.compile(
    r'\[(?:PERSON|JUDGE|ORGANIZATION|LOCATION|IdentityNumber)\]'
)

# Patterns for public/institutional entities — should NOT be anonymized in parte column
_ORG_PATTERN_RE = re.compile(
    r'(?i)(?:'
    r'fazenda|estado\s+de\b|munic[ií]p|prefeitura|secretári[oa]|'
    r'hospital|santa\s+casa|justiça|ministério|procuradoria|defensoria|'
    r'\bINSS\b|\bDETRAN\b|\bCETESB\b|\bSABESP\b|\bCPFL\b|'
    r'\bunimed\b|\bamil\b|bradesco\s+sa[úu]de|sul\s*am[ée]rica|notre\s*dame|hapvida|'
    r'uni[ãa]o\s+federal|governo|c[âa]mara\s+municipal|'
    r'funda[çc][ãa]o|instituto|universidade|faculdade|'
    r'\bltda\b|\bs[./]a\.?\b|\beireli\b|[\s-]me\s*$|[\s-]epp\s*$|'
    r'banco|caixa\s+econ|'
    r'delegacia|pol[íi]cia|'
    r'tribunal|ju[íi]zo|vara\b|comarca|'
    r'conselho|comiss[ãa]o|autarquia|ag[êe]ncia|'
    r'associa[çc][ãa]o|sindicato|cooperativa|federa[çc][ãa]o|'
    r'p[úu]blic[oa]|seguros?\b|seguradora|previd[êe]ncia|'
    r'servi[çc]o\s+(?:social|aut[ôo]nomo)|\bSESI\b|\bSENAI\b|\bSESC\b|'
    r'assist[êe]ncia|farmac[êe]utic|laborat[óo]rio|'
    r'sociedade|companhia|empresa'
    r')'
)

# Separator / empty values to skip
_EMPTY_RE = re.compile(r'^[\s\-—–]*$')


def sanitize_column(series: pd.Series, col_type: str) -> tuple:
    """Sanitize a parsed CSV column by replacing leaked names.

    Args:
        series: The pandas Series to sanitize.
        col_type: One of "person" (advogado), "judge" (juiz/relator/revisor),
                  or "party" (parte).

    Returns:
        Tuple of (sanitized Series, number of replacements made).
    """
    if col_type == "judge":
        placeholder = "[JUDGE]"
    else:
        placeholder = "[PERSON]"

    result = series.copy()
    count = 0

    for idx, val in result.items():
        if pd.isna(val):
            continue
        val_str = str(val).strip()
        if not val_str or _EMPTY_RE.match(val_str):
            continue

        if _ANON_TOKEN_RE.match(val_str):
            continue

        if col_type == "party":
            if _CONTAINS_ANON_RE.search(val_str):
                continue
            if _ORG_PATTERN_RE.search(val_str):
                continue

        result.at[idx] = placeholder
        count += 1

    return result, count


def sanitize_parsed_csvs(parsed_dir, grau: int = None, verbose: bool = True):
    """Sanitize name columns in parsed TJSP consulta CSVs.

    Args:
        parsed_dir: Path to directory containing parsed CSVs.
        grau: If specified, only sanitize files for this grau (1 or 2).
              If None, sanitize both.
        verbose: Print summary of changes.
    """
    parsed_dir = Path(parsed_dir)

    graus = [grau] if grau else [1, 2]

    rules = [
        ("adv_grau{grau}.csv", "advogado", "person"),
    ]
    grau1_rules = [
        ("proc_grau1.csv", "juiz", "judge"),
    ]
    grau2_rules = [
        ("proc_grau2.csv", "relator", "judge"),
        ("proc_grau2.csv", "revisor", "judge"),
    ]
    party_rules = [
        ("parte_grau{grau}.csv", "parte", "party"),
    ]

    total_sanitized = 0

    for g in graus:
        tasks = []
        for pattern, col, ctype in rules + party_rules:
            tasks.append((pattern.format(grau=g), col, ctype))
        if g == 1:
            tasks.extend([(f, c, t) for f, c, t in grau1_rules])
        if g == 2:
            tasks.extend([(f, c, t) for f, c, t in grau2_rules])

        for filename, col, ctype in tasks:
            csv_path = parsed_dir / filename
            if not csv_path.exists():
                continue

            df = pd.read_csv(csv_path)
            if col not in df.columns:
                continue

            df[col], count = sanitize_column(df[col], ctype)
            if count > 0:
                df.to_csv(csv_path, index=False)
                total_sanitized += count
                if verbose:
                    print(f"  Sanitized {count} values in {filename}:{col}")

    if verbose and total_sanitized == 0:
        print("  No additional names to sanitize")

    return total_sanitized


# ---------------------------------------------------------------------
# File and directory anonymization
# ---------------------------------------------------------------------

def anonymize_file(input_path, output_path, verbose: bool = True) -> bool:
    """Anonymize a single text file.

    Args:
        input_path: Path to input file
        output_path: Path to output file
        verbose: Print progress

    Returns:
        True if successful, False otherwise
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    try:
        content = input_path.read_text(encoding="utf-8")
        anonymized = anonymize_text(content)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(anonymized, encoding="utf-8")

        if verbose:
            print(f"  {input_path.name} -> {output_path.name}", flush=True)
        return True

    except Exception as e:
        print(f"  ERROR: {input_path.name}: {e}", flush=True)
        return False


def anonymize_directory(
    input_dir,
    output_dir,
    extensions: tuple = (".md", ".txt", ".html"),
    verbose: bool = True,
    incremental: bool = True,
) -> tuple:
    """Anonymize all matching files in a directory (recursively).

    Args:
        input_dir: Input directory
        output_dir: Output directory
        extensions: File extensions to process
        verbose: Print progress
        incremental: Skip files that already exist in output and are newer than input

    Returns:
        Tuple of (success_count, error_count, skipped_count)
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    success = 0
    errors = 0
    skipped = 0

    files = []
    for ext in extensions:
        files.extend(input_dir.rglob(f"*{ext}"))
    files = sorted(files)

    if not files:
        print(f"No files found with extensions {extensions}", flush=True)
        return 0, 0, 0

    print(f"Found {len(files)} files", flush=True)

    for i, input_path in enumerate(files, 1):
        rel_path = input_path.relative_to(input_dir)
        output_path = output_dir / rel_path

        if incremental and output_path.exists():
            if output_path.stat().st_mtime >= input_path.stat().st_mtime:
                skipped += 1
                continue

        if verbose:
            print(f"[{i}/{len(files)}] {rel_path}", flush=True)

        if anonymize_file(input_path, output_path, verbose=False):
            success += 1
        else:
            errors += 1

    return success, errors, skipped


def anonymize_csv(
    input_path,
    output_path,
    columns: List[str],
    verbose: bool = True,
) -> bool:
    """Anonymize specified columns in a CSV file.

    Args:
        input_path: Path to input CSV
        output_path: Path to output CSV
        columns: List of column names to anonymize
        verbose: Print progress

    Returns:
        True if successful, False otherwise
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    try:
        df = pd.read_csv(input_path)
        df = anonymize_dataframe(df, columns, verbose=verbose)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

        if verbose:
            print(f"Saved: {output_path}", flush=True)
        return True

    except Exception as e:
        print(f"ERROR: {e}", flush=True)
        return False


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():
    """Command-line interface for anonymization."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m diarios.anonymize",
        description="Anonymize text files, directories, or CSVs (Brazilian court text).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Anonymize a directory of .md/.txt/.html files
    python -m diarios.anonymize -i raw_dir -o anon_dir

    # Anonymize a CSV file
    python -m diarios.anonymize -i data.csv -o data_anon.csv --columns text notes

    # Anonymize a single file
    python -m diarios.anonymize -i file.md -o file_anon.md

Set DIARIOS_HF_CACHE_DIR to a directory of pre-downloaded HF models to avoid
network access on first run.
        """,
    )

    parser.add_argument("--input", "-i", type=Path, required=True, help="Input file or directory")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Output file or directory")
    parser.add_argument(
        "--columns", "-c", nargs="+",
        help="For CSV files: column names to anonymize",
    )
    parser.add_argument(
        "--ext", nargs="+", default=[".md", ".txt", ".html"],
        help="File extensions to process for directory mode (default: .md .txt .html)",
    )
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress progress output")
    parser.add_argument(
        "--full", action="store_true",
        help="Process all files, even if already anonymized (disable incremental)",
    )

    args = parser.parse_args()
    verbose = not args.quiet

    print("=" * 60, flush=True)
    print("Anonymization", flush=True)
    print("=" * 60, flush=True)
    print(f"Input:  {args.input}", flush=True)
    print(f"Output: {args.output}", flush=True)

    if args.input.suffix == ".csv":
        if not args.columns:
            print("\nERROR: --columns required for CSV files", flush=True)
            sys.exit(1)

        print(f"Columns: {args.columns}", flush=True)
        print("\nAnonymizing CSV...", flush=True)

        if anonymize_csv(args.input, args.output, args.columns, verbose=verbose):
            print("\nDone!", flush=True)
        else:
            sys.exit(1)

    elif args.input.is_file():
        print("\nAnonymizing file...", flush=True)
        if anonymize_file(args.input, args.output, verbose=verbose):
            print("\nDone!", flush=True)
        else:
            sys.exit(1)

    elif args.input.is_dir():
        extensions = tuple(args.ext)
        incremental = not args.full
        print(f"Extensions: {extensions}", flush=True)
        print(f"Mode: {'incremental (skip existing)' if incremental else 'full (process all)'}", flush=True)
        print("\nAnonymizing directory...", flush=True)

        success, errors, skipped = anonymize_directory(
            args.input,
            args.output,
            extensions=extensions,
            verbose=verbose,
            incremental=incremental,
        )

        print("\n" + "=" * 60, flush=True)
        print(f"Complete: {success} processed, {skipped} skipped, {errors} errors", flush=True)
        print(f"Output: {args.output}", flush=True)
        print("=" * 60, flush=True)

        if errors > 0:
            sys.exit(1)

    else:
        print(f"\nERROR: Input not found: {args.input}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
