"""Decision parsing and sentence extraction for court rulings."""

from diarios.decision.config import *
from diarios.decision.parser import *

__all__ = [
    "DecisionParser",
    "clean_sentenca_text",
    "get_main_sentence_regexes",
    "get_dispositivo_regexes",
    "get_desfecho_regexes",
    "get_mode",
    "get_key_order",
    "get_subject",
    "print_truncated",
    "intersect",
    "get_pena_regexes",
    "get_split_keys",
    "remove_pena_base",
    "get_df",
]
