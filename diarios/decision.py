import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.clean import extractall_series
from diarios.clean import split_series
from diarios.clean import get_cardinal_number_regex
from diarios.clean import extract_number
from diarios.parse import extract_regexes

# TODO: refactor/clean code
# TODO: test for Ap/STJ/STF decisions in APN
# TODO: if condenado and no mentioned pena, then get commonly extracted pena?

def clean_sentenca_text(text):
    return clean_text(text, drop="^A-Z0-9;:., \n\-")


def get_main_sentence_regexes():
    regexes = [
        r'julgo\b[^.]+',
        'decide.{0,15}turma[^.]+',
        'a turma.{0,10}(unanimidade|maioria)[^.]+',
        r'\.\s+(nego|dou)\b[^.]+',
        r'(para.{0,7}(condenar|absolver)|condeno|absolvo)\b[^.]+',        
        'retirado\s+d[ae]\s+pauta[^.]+',
        '(pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto[^.]+',
        'em\s+face.{0,15}considerações[^.]+',
        'is[ts]o.{0,10}posto[^.]+',
        '(diante|posto).{0,5}is[st]o[^.]+',
        'após o voto[^.]+',
        'proferido.{0,10}relatório[^.]+',
    ]
    regexes = [f'(?i)(?s)({regex})' for regex in regexes]
    return regexes


def get_dispositivo_regex():
    regexes = [
        'decide.{0,15}turma.*',
        'a turma.{0,10}(?:unanimidade|maioria).*',
        'retirado\s+d[ae]\s+pauta.*',
        '(?:pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto.*',
        'em\s+face.{0,15}considerações.*',
        'is[ts]o.{0,10}posto.*',
        '(?:diante|posto).{0,5}is[st]o.*',
        'após o voto.*',
        'proferido.{0,10}relatório.*',
        r'\.\s+(?:nego|julgo|dou|absolvo|condeno)\b.*',
    ]
    regex = '|'.join(regexes)
    return f'(?i)(?s)(?P<text>{regex})'


def get_mode():
    return {
        'UNANIMIDADE': 'UNANIMIDADE',
        'MAIORIA': 'MAIORIA',
    }
    

def get_key_order(key, order):
    order = {k: i for i, k in enumerate(order)}
    return key.map(order)

def get_desfecho_regexes(classes):
    if type(classes) == str:
        classes = [classes]
    regexes = {}
    for classe in classes:
        regexes = {**regexes, **_get_desfecho_regexes(classe)}
    return regexes


def _get_desfecho_regexes(classe):
    regexes = dict()
    if classe in ["APN", "ProOrd", "ACIA", "ACP"]:
        regexes = {
            **regexes,
            'PARCIALMENTE PROCEDENTE': 'PARCIALMENTE PROCEDENTE-',
            'PROCEDENTE EM PARTE': 'PARCIALMENTE PROCEDENTE-',
            'JULGO PROCEDENTE': 'PROCEDENTE-',
            'JULGO IMPROCEDENTE': 'IMPROCEDENTE-',
            'JULGO EXTINTO|EXTINGO': 'EXTINTO-',
        }
    if classe in ["ED"]:
        regexes = {
            **regexes,
            'REJEIT.*EMBARGOS.*DECL':
            'REJEITAR-EMBARGOS DE DECLARACAO',
        }
    if classe in ["RE"]:
        regexes = {
            **regexes,
            'NAO ADMIT.*RECURSO EXTRAORDINARIO':
            'NAO ADMITIR-RECURSO EXTRAORDINARIO',
            'ADMIT.*RECURSO EXTRAORDINARIO':
            'ADMITIR-RECURSO EXTRAORDINARIO',
        }
    if classe in ["APN"]:
        regexes = {
            **regexes,
            '(DECLAR|JULG|DECRET).*PRESCRI':
            'PRESCRICAO-',
            '(DECLAR|JULG|DECRET)[^.]*EXTIN[^.]*(PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO-',
            'ACOLH.*DENUNCIA': 'ACOLHER-DENUNCIA',
            'CONDEN(O|AR)': 'CONDENAR',
            'ABSOLV(O|ER)': 'ABSOLVER',
        }
    if classe in ["Ap"]:
        for obj in ["APELACAO",
                    "APELACOES",
                    "REMESSA OFICIAL",
                    "REMESSA NECESSARIA"]:
            regexes = {
                **regexes,            
                f'D(OU|EU|AR) PROVIMENTO PARCIAL.*{obj}':
                f'PARCIAL PROVIMENTO-{obj}',
                f'D(OU|EU|AR) PARCIAL PROVIMENTO.*{obj}':
                f'PARCIAL PROVIMENTO-{obj}',
                f'D(OU|EU|AR) PROVIMENTO.*{obj}': f'PROVIMENTO-{obj}',
                f'NEG.*PROVIMENTO.*{obj}': f'IMPROVIMENTO-{obj}',
                f'NAO CONHEC.*{obj}': f'NAO CONHECER-{obj}',
            }
    if classe in ["ACIA"]:
        regexes = {
            **regexes,            
            'RECEBO.*INICIAL': 'RECEBER-INICIAL',
            'REJEITO.*INICIAL': 'REJEITAR-INICIAL',
        }
    if classe in ["REsp"]:
        regexes = {
            **regexes,
            'NAO ADMIT.*RECURSO ESPECIAL':
            'NAO ADMITIR-RECURSO ESPECIAL',
            'ADMIT.*RECURSO ESPECIAL':
            'ADMITIR-RECURSO ESPECIAL',
        }
    return regexes


def get_subject():
    return {
        'TURMA': 'TURMA',
        'CAMARA': 'CAMARA',
    }


class DecisionParser:
    """Class to parse decisions"""

    def __init__(
            self,
            text,
            parte=None,
            classes=["ProOrd", "ACIA", "APN", "ED", "Ap"],
            remove_dots=[r'\barts?', r'\bfls?', '[0-9]'],
            key_order=['PRESCRICAO', 'ABSOLVO', 'CONDENO'],
            split_desfecho=True,
            main_sentence_regexes=get_main_sentence_regexes(),
            dispositivo_regex=get_dispositivo_regex(),
            subject=get_subject(),
            mode=get_mode(),
    ):
        self.text = _clean_text(text, remove_dots)
        self.main_sentence_regexes = main_sentence_regexes
        self.subject = subject
        self.mode = mode
        self.parte = parte
        self.dispositivo_regex = dispositivo_regex
        self.classes = classes
        self.text_pena_cols = ['regime']
        self.split_desfecho = split_desfecho
        self.key_order = key_order

    def parse(self):
        df = pd.DataFrame(index=self.text.index)
        df['main_sentence'] = pd.NA
        for regex in self.main_sentence_regexes:
            empty = df.main_sentence.isnull()
            df.loc[empty, 'main_sentence'] = (
                self.text.loc[empty]
                .str.extract(regex)[0]
            )
        df['main_sentence'] = clean_text(df.main_sentence)
        regexes = get_desfecho_regexes(self.classes)
        desfecho = map_regex(
            df.main_sentence,
            regexes,
            keep_unmatched=False
        )
        if self.split_desfecho:
            desfecho = desfecho.str.split('-', expand=True)
            desfecho.columns = ['verb', 'object']
        else:
            desfecho = desfecho.str.replace('-', ' ').str.strip()
            desfecho.name = 'desfecho'
        df = df.join(desfecho, how='left')
        if self.mode:
            df["mode"] = map_regex(
                df.main_sentence,
                self.mode,
                keep_unmatched=False
            )
        if self.subject:
            df["subject"] = map_regex(
                df.main_sentence,
                self.subject,
                keep_unmatched=False
            )
        self.parsed = df
        return df

    def parse_parte(self):
        self.dispositivo = clean_sentenca_text(
            self.text
            .str.extract(self.dispositivo_regex)
            .text
        )
        single_parte = self.parte.groupby(parte.index.name).size() == 1
        parsed1 = self._parse_single_parte(self.dispositivo.loc[single_parte])
        parsed2 = self._parse_multiple_partes(self.dispositivo.loc[~single_parte])
        parsed_parte = pd.concat([parsed1, parsed2])
        parsed_parte = self._drop_duplicate_parte(parsed_parte)
        self.parsed_parte = parsed_parte
        return parsed_parte

    def _parse_single_parte(self, text):
        text = remove_pena_base(text)
        keys = get_split_keys(self.classes)
        df = pd.DataFrame({'text': text})
        df['key'] = map_regex(text, keys, keep_unmatched=False)
        df = df.query('key.notnull()')
        df = self._add_penas(df)
        return df

    def _add_penas(self, df):
        regexes, boolean_regexes, cols = get_pena_regexes(self.classes)
        pena = extract_regexes(df.text, regexes, update=True)
        df = df.join(pena)
        num_cols = set(cols) - set(self.text_pena_cols)
        for c in num_cols:
            if c in df.columns:
                df[c] = extract_number(df[c])
        for k, v in boolean_regexes.items():
            df[k] = df.text.str.contains(v) * 1
        df = self._clean_penas(df)
        return df

    def _clean_penas(self, df):
        if intersect(["ACIA", "APN"], self.classes):
            _, _, pena_cols = get_pena_regexes(self.classes)
            for c in pena_cols:
                if c in df.columns:
                    df.loc[df.key != "CONDENO", c] = pd.NA
        if "APN" in self.classes:
            df.loc[df.regime.fillna('').str.contains('SEMI'), 'regime'] = "SEMIABERTO"
            df['prisao_dias'] = df.years.fillna(0)*365 + df.months.fillna(0)*30 + df.days.fillna(0)
            df['diasmulta'] = df.diasmulta.fillna(df.diasmulta2)
            df = df.drop(columns='diasmulta2')
        return df

    def _parse_multiple_partes(self, text):
        self.splitted = self._split_on_key(text)
        splitted = self._add_parte_regex(self.splitted)
        df = extractall_series(splitted.text, splitted.parte_regex)
        df = df.join(splitted.key, how='inner')
        df['text'] = remove_pena_base(df.text)
        df = self._add_penas(df)
        df = df.reset_index(['group', 'match'], drop=True)
        return df

    def _add_parte_regex(self, splitted):
        parte_regex = get_parte_regex(self.parte)
        parte_regex = (
            r"(?s)(?P<text>\b(?P<parte>" + parte_regex + r")\b.*?)" +
            "(?=" + parte_regex + "|$)"
        )
        return splitted.join(parte_regex, how='inner')
    
    def _split_on_key(self, text):
        # Splits on condeno, absolvo, etc
        keys = get_split_keys(self.classes)
        regex = r"(?i)\b(?P<key>{})\b".format('|'.join(keys.keys()))
        df = split_series(text, regex, drop_end=True)
        df['key'] = map_regex(df.key, keys)
        return df
    
    def _drop_duplicate_parte(self, df):
        _, _, pena_cols = get_pena_regexes(self.classes)
        pena_cols = set(df.columns).intersection(pena_cols)
        df['has_pena'] = df[pena_cols].notnull().sum(axis=1) > 0
        df['key_order'] = get_key_order(df.key, self.key_order)
        df['has_key'] = df.key.notnull() # Should not be necessary
        sort_cols = ['has_key', 'has_pena', 'key_order']
        ix_name = df.index.name
        df = (
            df
            .sort_values(sort_cols)
            .reset_index()
            .drop_duplicates([ix_name, 'parte'], keep='last')
            .set_index(ix_name)
            .drop(columns=['has_pena', 'key_order', 'has_key'])
        )
        return df

    def test(self, regex=None, max_str=2000, max_str_sentence=1000):
        if regex:
            text = self.text.loc[self.text.str.contains(regex)]
        else:
            text = self.text
        sm = text.sample().index[0]
        parsed = self.parsed.loc[sm]
        print("TEXT:", self.text.loc[sm][-max_str:])
        print('')
        print("MAIN SENTENCE:", parsed.main_sentence[0:max_str_sentence])
        print('')
        desfecho = ' '.join(
            parsed
            .drop('main_sentence')
            .fillna('')
            .tolist()
        )
        print("DESFECHO:", desfecho)
        return sm

    def test_parte(self, regex=None, max_str=2000, max_str_dispositivo=1000, max_str_pena=1000):
        if regex:
            text = self.text.loc[self.text.str.contains(regex)]
        else:
            text = self.text
        sm = text.sample().index[0]
        print(self.text.loc[sm][-max_str:])
        print('')
        print("DISPOSITIVO")
        print('')
        print(self.dispositivo.loc[sm][-max_str_dispositivo:])
        print('')
        try:
            partes = self.parte.loc[sm]
            if type(partes) != str:
                partes = ', '.join(partes.tolist())
            print("PARTES:", partes)
        except KeyError:
            print("NO PARTES")
        print('')
        try:
            print(self.splitted.loc[sm].transpose())
        except KeyError:
            print("Nothing extracted")
        print('')
        print('PENAS')
        print('')
        try:
            penas = self.parsed_parte.loc[sm]
            if type(penas) == pd.core.series.Series:
                print(penas.text[-max_str_pena:])
                print(penas)
            else:
                for _, row in self.parsed_parte.loc[sm].iterrows():
                    print(row.text[-max_str_pena:])
                    print(row)
        except KeyError:
            print("Nothing extracted")
        return sm


def intersect(list1, list2):
    return len(set(list1).intersection(list2)) > 0


def _clean_text(text, remove_dots):
    for r in remove_dots:
        text = text.str.replace(f'({r})\.', r'\1', regex=True)
    return text


def get_parte_regex(parte):
    parte = clean_text(parte) + '|'
    parte = parte.loc[parte.notnull()]
    regex = parte.groupby(parte.index.name).sum()
    regex = regex.str.replace(r"\|$", "", regex=True)
    regex.name = 'parte_regex'
    return regex


def get_pena_regexes(classes=['APN', 'ACIA']):
    n = get_cardinal_number_regex()
    s = '.{0,5}'
    m = '.{0,10}'
    l = '.{0,40}'
    years = f'(?P<years>{n}){m}\\bANOS?\\b'
    months = f'(?P<months>{n}){m}\\bMES(ES)?\\b'
    days = f'(?P<days>{n}){m}\\bDIAS?\\b'
    tp = '(DETENCAO|RECLUSAO)'
    regexes = [
        f'MULTA{m}\\bR\s*(?P<multaR>{n})'
    ]
    boolean_regexes = {
        'has_multa': 'MULTA',
    }
    if 'APN' in classes:
        regexes += [
            f'{years}{s}({months}{s}({days}{s})?)?{tp}',
            f'{months}{s}({days}{s})?{tp}',
            f'REGIME{l}?(?P<regime>FECHADO|SEMI{s}ABERTO|ABERTO)',
            f'(?P<diasmulta>{n}){l}DIAS{s}MULTA',
            f'DIAS{s}MULTA{m}(?P<diasmulta2>{n})',
        ]
    if 'ACIA' in classes:
        regexes += [
            f'PROIB{m}CONTRATAR[^.]+(?P<contratar>{n}){s}ANOS',
            f'DIREITOS{s}POLITICOS[^.]+(?P<dirpol>{n}){s}ANOS',
            f'RESSARCIR[^.]+\\bR\\b{s}(?P<ressarcirR>{n})',
        ]
        boolean_regexes = {
            **boolean_regexes,
            'perda_funcao': f'PERDA.{l}FUNCAO',        
            'ressarcir': 'RESSARCI|RESTITUICAO',
        }
    cols = (
        list(boolean_regexes.keys()) +
        pd.Series(regexes).str.extractall('<(.*?)>')[0].tolist()
    )
    return regexes, boolean_regexes, cols



def get_split_keys(classes):
    keys = {}
    for classe in classes:
        keys = {**keys, **_get_split_keys(classe)}
    return keys


def _get_split_keys(classe):
    keys = {}
    if classe in ["ACIA", "APN"]:
        keys = {
            **keys,
            'CONDEN(?:O|AR)': 'CONDENO',
            'ABSOLV(?:O|ER)': 'ABSOLVO',
        }
    if classe == "APN":
        keys = {
            **keys,
            'DOSIMETRIA': 'CONDENO',
            '(?:DECLARO|DECRETO|JULGO)[^.]*EXTIN[^.]*(?:PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO',
            '(?:JULGO|DECLARO|VERIFICO)[^.]*PRESCRICAO': 'PRESCRICAO',
            'PRONUNCI(?:O|AR)': 'PRONUNCIO',
        }
    return keys


def remove_pena_base(text):
    # Removing "FIXO PENA BASE DE 2 ANOS" etc
    regex = '(?s)(^.*)((TORN|FIX).{0,15}DEFINITIV[OA]|TOTALIZ(O|AM))'
    return text.str.replace(regex, r'\2', regex=True) 


# Not done: Differentiate between reclusao and detencao (sometimes 2 anos reclusao e 3 meses detencao)



def get_df():
    df = pd.read_csv('../audit/build/clean/TRF1_inteiro_teor_text.csv')
    df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)condeno')]
    df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)deten[cç][aã]o|reclus[aã]o')]
    df2 = df.drop_duplicates('num_npu').sample(100)
    df2 = df2.set_index('num_npu')
    parte = pd.read_csv('../audit/build/clean/TRF1_parte.csv')
    parte = parte.loc[parte.key.isin(['REU', 'REQDO'])]
    parte = parte.drop_duplicates(['num_npu', 'parte'])
    parte = parte.set_index('num_npu')
    parte = parte.join(df2, lsuffix='1')
    parte['has_parte'] = 1
    df2['has_parte'] = parte.groupby('num_npu').has_parte.sum() > 0
    df2 = df2.query('has_parte==True')
    return df2, parte

#df, parte = get_df()

parser = DecisionParser(df.inteiro_teor, parte.parte)        

out = parser.parse_parte()
sm = parser.test_parte()

#out = parser.parse()
#sm = parser.test()
