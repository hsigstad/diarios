"""Configuration utilities for reading user-config YAML files."""

from __future__ import annotations

from typing import Any

import yaml


def get_user_config(key: str) -> Any:
    """Read a value from the user-config.yaml file.

    Args:
        key: Top-level key to look up in the YAML file.

    Returns:
        The value associated with the given key.
    """
    with open('user-config.yaml', 'r') as stream:
        data = yaml.load(stream, Loader=yaml.FullLoader)
    return data[key]
