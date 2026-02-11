"""Hierarchical text parser using regex-based structure definitions."""

from __future__ import annotations

from typing import Any, List, Optional

import re
import warnings
import pandas as pd
import os
import copy

__all__ = [
    "Structure",
    "bold",
    "parse",
    "parse_structure_string",
]


class Structure:
    """A tree node representing a section of hierarchically structured text.

    Each node can have a regex that matches a section header, optional
    children representing sub-sections, and the text content between
    the current header and the next sibling.

    Args:
        key: Human-readable label for this section.
        regex: Regex pattern that marks the start of this section.
        multiple_matches: If True, this child pattern can repeat.
        children: List of child Structure nodes.
        text: Text content assigned to this node after parsing.
        regex_match: The actual text that matched the regex.
        text_repr_length: Max characters of text to show in ``__repr__``.
        level: Nesting depth in the tree (used for display indentation).
    """

    def __init__(
            self,
            key: Optional[str] = None,
            regex: Optional[str] = None,
            multiple_matches: bool = False,
            children: Optional[List[Structure]] = None,
            text: Optional[str] = None,
            regex_match: Optional[str] = None,
            text_repr_length: int = 25,
            level: int = 0
        ) -> None:
        self.key = key
        self.regex = regex
        self.text = text
        self.multiple_matches = multiple_matches
        self.regex_match = regex_match
        self.level = level
        self.children = children
        self.text_repr_length = text_repr_length

    def add_child(self, child: Structure) -> None:
        """Append a child node to this structure.

        Args:
            child: Structure node to add as a child.
        """
        if self.children:
            self.children += [child]
        else:
            self.children = [child]

    def extract(self, path: List[str]) -> pd.DataFrame:
        """Extract matching nodes by walking a path of regex patterns.

        Args:
            path: List of regex patterns to match against child keys
                at each depth level (e.g., ``['zonas', 'Zona.*', 'editais']``).

        Returns:
            DataFrame with columns ``text``, ``regex_match``, ``key``, ``regex``
            for each matching leaf node.
        """
        # path is a list of regexes
        # e.g. ['zonas', 'Zona*', 'editais']
        if len(path) == 0:
            return pd.DataFrame([{
                'text': self.text,
                'regex_match': self.regex_match,
                'key': self.key,
                'regex': self.regex
            }])
        children = [
            child for child
            in self.children
            if re.match(path[0], child.key)
        ]
        path.pop(0)
        func = lambda x: x.extract(path)
        return pd.concat(map(func, children))

    def parse(self, text: str) -> None:
        """Parse text into this structure tree.

        Assigns text to this node and recursively propagates to children.

        Args:
            text: Full text to parse.
        """
        self.text = text
        self._propagate_text()

    def _propagate_text(self) -> None:
        """Recursively propagate text from this node to its children."""
        if self.children:
            self._propagate_text_to_children()
            for child in self.children:
                child._propagate_text()

    def _propagate_text_to_children(self) -> None:
        """Split this node's text among its children using their regexes."""
        if not self.text:
            return
        last_match = None
        text = self.text
        child1 = self.children[0]
        if child1.multiple_matches:
            if len(self.children) > 1:
                raise Exception(
                    'Can only have one '
                    'multiple child'
                )
            self._match_multiple(
                child1, last_match, text
            )
        else:
            for child in self.children:
                _, last_match, text = self._match(
                    child, last_match, text
                )

    def _match_multiple(
            self, child: Structure,
            last_match: Optional[Structure], text: str
        ) -> tuple:
        """Match a child pattern repeatedly against the remaining text.

        Args:
            child: Child node whose regex can match multiple times.
            last_match: The previously matched sibling (or None).
            text: Remaining text to match against.

        Returns:
            Tuple of (last_match, remaining_text).
        """
        match = True
        while match:
            match, last_match, text = (
                self._match(
                    child, last_match, text
                )
            )
            if match:
                child = copy.copy(child)
                self.add_child(child)
        self.children.pop()
        return last_match, text


    def _match(
        self,
        child: Structure,
        last_match: Optional[Structure],
        text: str,
    ) -> tuple:
        """Try to match a child's regex against the text.

        Args:
            child: Child node to match.
            last_match: Previously matched sibling whose text gets updated.
            text: Remaining text to search.

        Returns:
            Tuple of (match_object, last_match, remaining_text).
        """
        match = re.match(
            '(?s)(.*?)({})(.*)'.format(
                child.regex
            ),
            text
        )
        if match:
            g1, g2, g3 = match.groups()
            child.regex_match = g2
            if not last_match:
                self.text = g1
            else:
                last_match.text = g1
            child.text = g3
            text = g3
            last_match = child
        return match, last_match, text

    def save(self, outfile: str) -> None:
        """Save the structure tree as an Org-mode file.

        Args:
            outfile: Output file path (must end in ``.org``).

        Raises:
            Exception: If the file extension is not ``.org``.
        """
        extension = re.search(
            '\.[a-z]{2,3}$',
            outfile
        ).group(0)
        if extension != '.org':
            raise Exception(
                'Can only save to org'
            )
        org = self.to_org()
        with open(outfile, 'w') as f:
            f.write(org)

    def to_org(self) -> str:
        """Convert this structure tree to Org-mode formatted text.

        Returns:
            Org-mode string with headings based on tree depth.
        """
        if self.regex_match:
            org = '{} {}\n\n{}\n{}'.format(
                '*'*(self.level + 1),
                self.key,
                self.regex_match,
                self.text
            )
        else:
            org = ''
        if self.children:
            children_org = '\n'.join([
                child.to_org()
                for child in self.children
            ])
            org = '{}{}'.format(
                org, children_org
            )
        return org

    def __repr__(self) -> str:
        indent = ' '*self.level
        base = '\n{}{}\n{}{}'.format(
            indent, bold(self.key),
            indent, self.regex
        )
        if self.text:
            text = self.text[
                0:self.text_repr_length
            ].replace('\n', ' ').strip()
            base = '{}\n{}{}...'.format(
                base, indent, text
            )
        if self.children:
            for child in self.children:
                base = '{}\n{}'.format(
                    base, child
                )
        return '{}'.format(base)


def bold(str: str) -> str:
    """Wrap a string in ANSI bold escape codes.

    Args:
        str: Text to make bold.

    Returns:
        Bold-formatted string for terminal display.
    """
    return '\033[1m{}\033[0m'.format(
        str
    )


def parse(text: str, structure: str) -> Structure:
    """Parse text using a structure definition string.

    Args:
        text: Text to parse.
        structure: Multi-line structure definition (see ``parse_structure_string``).

    Returns:
        Populated Structure tree with parsed text.
    """
    structure = parse_structure_string(
        structure
    )
    structure.parse(text)
    return structure


def parse_structure_string(structure_string: str) -> Structure:
    """Convert a structure definition string into a Structure tree.

    The string format uses indentation to indicate nesting, with each line
    containing ``key,regex``. Append ``X`` to the key for multiple matches.

    Args:
        structure_string: Multi-line structure definition.

    Returns:
        Root Structure node with children built from the definition.
    """
    df = _structure_string_to_df(
        structure_string
    )
    structure = list(df.apply(
        lambda row: Structure(
            key=row.key,
            regex=row.regex,
            level=row.level,
            multiple_matches=row.multiple_matches
        ),
        axis=1
    ))
    _build_structure_tree(structure)
    structure = Structure(
        children=structure
    )
    return structure


def _structure_string_to_df(string: str) -> pd.DataFrame:
    """Parse a structure definition string into a DataFrame.

    Args:
        string: Multi-line structure string.

    Returns:
        DataFrame with columns ``key``, ``regex``, ``level``, ``multiple_matches``.
    """
    df = pd.DataFrame(
        {'value': string.split('\n')}
    ).drop(0)
    df['value'] = df['value'].str.replace(
        '(^ *)([^ ])','\\1,\\2', regex=True
    )
    df = df['value'].str.split(
        ',', expand=True
    )
    df.columns = ['indent', 'key', 'regex']
    df['level'] = df['indent'].str.len()
    df['multiple_matches'] = df['key'].str.contains('X')
    return df


def _build_structure_tree(lst: List[Structure]) -> List[Structure]:
    """Build parent-child relationships in a flat list of Structure nodes.

    Nodes are linked based on their ``level`` attribute: a node at level N+1
    following a node at level N becomes its child.

    Args:
        lst: Flat list of Structure nodes ordered by appearance.

    Returns:
        The modified list (nodes are linked in place).
    """
    i = 0
    any_added = False
    child_added = True
    while child_added:
        added = _try_to_add_child(lst, i)
        if added:
            any_added = True
        i += 1
        if i > len(lst) - 2:
            i = 0
            child_added = any_added
            any_added = False
    return lst


def _try_to_add_child(lst: List[Structure], i: int) -> bool:
    """Try to make ``lst[i+1]`` a child of ``lst[i]`` based on level.

    Args:
        lst: List of Structure nodes.
        i: Index of the potential parent node.

    Returns:
        True if a child was added, False otherwise.
    """
    l = [l.level for l in lst[i:i+3]]
    cond = l[1] == l[0] + 1
    if len(l) == 3:
        cond = cond and l[2] <= l[1]
    if cond:
        lst[i].add_child(lst[i+1])
        lst.pop(i+1)
        return True
    else:
        return False


if __name__ == '__main__':
    structure = '''
aa,A
 bbb,B
  ddd,D
 ccc,C
dd,D
 ffX,F
ee,E
 gg,G'''
    text = (
        'aasdf A sa a fs b df '
        'B asf a d D sfads b saf C asfdds'
        'asdf sadf D  ggg F aasf F sdfd F ttt'
        'E safad'
    )
    struct = parse_structure_string(structure)
    struct.parse(text)
    df = struct.extract(['aa', '.*'])
    print(df)
    print(struct)
