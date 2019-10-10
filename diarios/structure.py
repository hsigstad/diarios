import re
import warnings
import pandas as pd
import os
import copy


class Structure:
    def __init__(
            self,
            key=None,
            regex=None,
            multiple_matches=False,
            children=None,
            text=None,
            regex_match=None,
            text_repr_length=25,
            level=0
        ):
        self.key = key
        self.regex = regex
        self.text = text
        self.multiple_matches = multiple_matches
        self.regex_match = regex_match
        self.level = level
        self.children = children
        self.text_repr_length = text_repr_length

    def add_child(self, child):
        if self.children:
            self.children += [child]
        else:
            self.children = [child]

    def extract(self, path):
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

    def parse(self, text):
        self.text = text
        self._propagate_text()

    def _propagate_text(self):
        if self.children:
            self._propagate_text_to_children()
            for child in self.children:
                child._propagate_text()

    def _propagate_text_to_children(self):
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
            self, child,
            last_match, text
        ):
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

                            
    def _match(self, child, last_match, text):
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

    def save(self, outfile):
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

    def to_org(self):
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
                                                   
    def __repr__(self):
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


def bold(str):
    return '\033[1m{}\033[0m'.format(
        str
    )


def parse(text, structure):
    structure = parse_structure_string(
        structure
    )
    structure.parse(text)
    return structure
    

def parse_structure_string(structure_string):
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
    

def _structure_string_to_df(string):
    df = pd.DataFrame(
        {'value': string.split('\n')}
    ).drop(0)
    df['value'] = df['value'].str.replace(
        '(^ *)([^ ])','\\1,\\2'
    )
    df = df['value'].str.split(
        ',', expand=True
    )
    df.columns = ['indent', 'key', 'regex']
    df['level'] = df['indent'].str.len()
    df['multiple_matches'] = df['key'].str.contains('X')
    return df


def _build_structure_tree(lst):
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


def _try_to_add_child(lst, i):
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
