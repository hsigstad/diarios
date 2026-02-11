"""Database connection and query utilities for SQLite, MySQL, and PostgreSQL."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from time import time
import sqlite3
import os
from re import sub
from diarios.misc import get_user_config

__all__ = [
    "query",
    "insert",
    "create_index",
    "connect",
    "get_db_engine",
    "get_postgresql_engine",
]


def query(
    database: Union[str, List[str]],
    sql: str,
    flavor: str = "sqlite3",
    echo: bool = True,
) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame.

    Args:
        database: Path to database file, or list of paths to attach multiple
            SQLite databases.
        sql: SQL query string to execute.
        flavor: Database backend: ``"sqlite3"``, ``"mysql"``, or ``"postgresql"``.
        echo: Whether to echo SQL statements (for SQLAlchemy engines).

    Returns:
        Query results as a DataFrame.
    """
    if isinstance(database, str):
        conn = connect(database, flavor, echo=echo)
    if isinstance(database, list):
        conn = sqlite3.connect(database[0])
        c = conn.cursor()
        for d in database[1:]:
            name = sub("\..*", "", os.path.basename(d))
            c.execute("ATTACH '{}' AS {}".format(d, name))
    return pd.read_sql(sql, conn)


def insert(
    database: str,
    table: str,
    files: List[str],
    columns: Optional[List[str]] = None,
    primary: Optional[str] = None,
    echo: bool = False,
    index: bool = False,
    fts5: bool = False,
    flavor: str = "sqlite3",
    truncate: Optional[Dict[str, int]] = None,
    chunksize: int = 100000,
    dtype_csv: Optional[dict] = None,
    **kwargs: Any,
) -> None:
    """Insert CSV files into a database table.

    Args:
        database: Path or name of the database.
        table: Target table name.
        files: List of CSV file paths to insert.
        columns: Column names to keep; missing columns are filled with NA.
        primary: Primary key column (unused, kept for API compat).
        echo: Whether to echo SQL statements.
        index: Whether to write the DataFrame index.
        fts5: If True, create an FTS5 virtual table.
        flavor: Database backend.
        truncate: Dict mapping column names to max string lengths.
        chunksize: Unused (kept for API compat).
        dtype_csv: Dtype dict passed to ``pd.read_csv``.
        **kwargs: Extra keyword arguments passed to ``DataFrame.to_sql``.
    """
    conn = connect(database, flavor, echo=echo)
    if_exists = "replace"
    if fts5:
        conn.execute("DROP TABLE IF EXISTS {};".format(table))
        conn.execute(
            "CREATE VIRTUAL TABLE {} USING FTS5 ({});".format(table, ",".join(columns))
        )
        if_exists = "append"
    for infile in files:
        print(infile)
        df = pd.read_csv(infile, dtype=dtype_csv, lineterminator="\n")
        for c in columns:
            if c not in df.columns:
                df[c] = pd.NA
        if truncate is not None:
            for k, v in truncate.items():
                df[k] = df[k].astype(str).str[0:v]
                df.loc[df[k] == "nan", k] = ""
        df = df.loc[:, columns]
        t0 = time()
        df.to_sql(table, conn, if_exists=if_exists, index=index, **kwargs)
        print("Duration:", round(time() - t0))
        if_exists = "append"


def create_index(
    database: str,
    table: str,
    columns: List[str],
    name: str,
    unique: bool = False,
    flavor: str = "sqlite3",
    fulltext: bool = False,
) -> None:
    """Create an index on a database table.

    Args:
        database: Path or name of the database.
        table: Table to index.
        columns: Columns to include in the index.
        name: Name for the index.
        unique: Whether to create a UNIQUE index.
        flavor: Database backend.
        fulltext: Whether to create a FULLTEXT index (MySQL/PostgreSQL).
    """
    if unique:
        pre = "UNIQUE"
    elif fulltext:
        pre = "FULLTEXT"
    else:
        pre = ""
    conn = connect(database, flavor)
    cols = ", ".join(columns)
    sql = "DROP INDEX {} ON {}".format(name, table)
    try:
        conn.execute(sql)
    except Exception:
        pass
    if flavor == "postgresql" and fulltext:
        sql = "CREATE INDEX {} ON {} USING GIN (to_tsvector('portuguese', {}))".format(
            name, table, cols
        )
    else:
        sql = "CREATE {} INDEX {} ON {} ({})".format(pre, name, table, cols)
    conn.execute(sql)


def connect(database: str, flavor: str, **kwargs: Any) -> Any:
    """Open a database connection.

    Args:
        database: Path or name of the database.
        flavor: Database backend: ``"sqlite3"``, ``"mysql"``, or ``"postgresql"``.
        **kwargs: Extra arguments forwarded to engine constructors.

    Returns:
        A database connection or SQLAlchemy engine.
    """
    if flavor == "sqlite3":
        conn = sqlite3.connect(database)
    if flavor == "mysql":
        conn = get_db_engine(database, **kwargs)
    if flavor == "postgresql":
        conn = get_postgresql_engine(database, **kwargs)
    return conn


def get_db_engine(database: str, echo: bool = True) -> Any:
    """Create a SQLAlchemy engine for MySQL.

    Args:
        database: Database name.
        echo: Whether to echo SQL statements.

    Returns:
        SQLAlchemy engine.
    """
    from sqlalchemy import create_engine
    user = get_user_config("mysql_user")
    pw = get_user_config("mysql_pw")
    host = get_user_config("mysql_host")
    engine = create_engine(
        "mysql+mysqlconnector://{0}:"
        "{1}@{2}/{3}?charset=utf8".format(user, pw, host, database),
        echo=echo,
    )
    return engine


def get_postgresql_engine(database: str, echo: bool = True) -> Any:
    """Create a SQLAlchemy engine for PostgreSQL.

    Args:
        database: Database name.
        echo: Whether to echo SQL statements.

    Returns:
        SQLAlchemy engine.
    """
    from sqlalchemy import create_engine
    user = get_user_config("postgresql_user")
    pw = get_user_config("postgresql_pw")
    host = get_user_config("postgresql_host")
    engine = create_engine(
        "postgresql+psycopg2://{}:{}@{}/{}".format(user, pw, host, database),
        echo=echo,
    )
    return engine
