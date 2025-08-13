import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.clean import extractall_series
from diarios.clean import split_series
from diarios.clean import get_cardinal_number_regex
from diarios.clean import extract_number
from diarios.parse import extract_regexes

# TODO: Differentiate between reclusao and detencao (sometimes 2 anos reclusao e 3 meses detencao)

def _clean_text(text, replace_text, remove_dots, remove_regexes):
    for k, v in replace_text.items():
        text = text.str.replace(k, v, regex=True)
    for r in remove_dots:
        text = text.str.replace(f'({r})\.', r'\1', regex=True)
    for regex in remove_regexes:
        text = text.str.replace(regex, '', regex=True)
        text = text.str.replace(regex, '', regex=True) # If matches twice
    text.name = 'text'
    return text


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


def get_dispositivo_regexes():
    regexes = [
        r'julgo\b.*',        
        'decide.{0,15}turma.*',
        'a turma.{0,10}(?:unanimidade|maioria).*',
        'retirado\s+d[ae]\s+pauta.*',
        '(?:pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto.*',
        'em\s+face.{0,15}considerações.*',
        'is[ts]o.{0,10}posto.*',
        '(?:diante|posto).{0,5}is[st]o.*',
        'após o voto.*',
        'proferido.{0,10}relatório.*',
        r'\b(?:nego|dou|absolvo|condeno|declaro|concedo)\b.*',
    ]
    regexes = [f'(?i)(?s)({regex})' for regex in regexes]
    return regexes


def get_desfecho_regexes(classes):
    if type(classes) == str:
        classes = [classes]
    regexes = {}
    for classe in classes:
        regexes = {**regexes, **_get_desfecho_regexes(classe)}
    return regexes


def get_mode():
    return {
        'UNANIMIDADE': 'UNANIMIDADE',
        'MAIORIA': 'MAIORIA',
    }
    

def get_key_order(key, order):
    order = {k: i for i, k in enumerate(order)}
    return key.map(order)




def _get_desfecho_regexes(classe):
    regexes = {
        'HOMOLOG(O|ASE).*DESISTENCIA':
        'HOMOLOGAR-DESISTENCIA',
    }
    if classe in ["all"]: # Not caring about object
        regexes = {
            **regexes,
            'PARCIALMENTE PROCEDENTE': 'PARCIALMENTE PROCEDENTE-',
            'PROCEDENTE EM PARTE': 'PARCIALMENTE PROCEDENTE-',
            'JULG(O|ASE) PROCEDENTE': 'PROCEDENTE-',
            'JULG(O|ASE) IMPROCEDENTE': 'IMPROCEDENTE-',
            'EXT.*SEM.*MERITO': 'S/MERITO-',
            'NAO (SE )?CONHEC': 'NAO CONHECER-',
            'JULG(O|ASE) EXTINTO|EXTINGO': 'EXTINTO-',
            '(INDEFIRO|INDEFERESE)': 'INDEFERIR-',
            r'\bDEFIRO': 'DEFERIR-',
            r'\b(DOU|JULGO|DECLARO|JULGASE).*PREJUDICAD': 'PREJUDICADO-',
            r'\bD(OU|EU|AR|ASE) (SEGU|PROV)IMENTO PARCIAL': 'PARCIAL PROVIMENTO-',
            r'\bD(OU|EU|AR|ASE) PARCIAL (SEGU|PROV)IMENTO': 'PARCIAL PROVIMENTO-',
            r'\bD(OU|EU|AR|ASE) (SEGU|PROV)IMENTO': 'PROVIMENTO-',
            r'\bNEG(AR|O|ASE).*(SEGU|PROV)IMENTO': 'IMPROVIMENTO-',
            'REJEITO': 'REJEITAR-',
            'ACOLH.*PARTE': 'ACOLHER EM PARTE-',
            'ACOLHO': 'ACOLHER-',
            r'CONCEDO': 'CONCEDER-',
            r'\b(NEGO|DENEGO)': 'NEGAR-',
            'ABSOLVO': 'ABSOLVER-',
            'CONDENO': 'CONDENAR-',
            '(NAO |IN)ADMITO': 'NAO ADMITIR-',
            'ADMITO': 'ADMITIR-',
            '(DECLAR|JULG|DECRET)[^.]*EXTIN[^.]*(PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO-',
        }
    if classe in ["APN", "ProOrd", "ACIA", "ACP"]:
        regexes = {
            **regexes,
            'PARCIALMENTE PROCEDENTE': 'PARCIALMENTE PROCEDENTE-',
            'PROCEDENTE EM PARTE': 'PARCIALMENTE PROCEDENTE-',
            'JULGO PROCEDENTE': 'PROCEDENTE-',
            'JULGO IMPROCEDENTE': 'IMPROCEDENTE-',
            'EXT.*SEM.*MERITO': 'S/MERITO-',
            'JULGO EXTINTO|EXTINGO': 'EXTINTO-',
        }
    if classe in ["ED"]:
        regexes = {
            **regexes,
            'REJEIT.*EMBARGOS.*DECL':
            'REJEITAR-EMBARGOS DE DECLARACAO',
            'REJEIT.*EMBARGOS':
            'REJEITAR-EMBARGOS',
            'ACOLH.*PARTE.*EMBARGOS.*DECL':
            'ACOLHER EM PARTE-EMBARGOS',
            'ACOLH.*EMBARGOS.*DECL':
            'ACOLHER-EMBARGOS',
        }
    if classe in ["Ag"]:
        obj = '(RECURSO|AGRAVO)'
        regexes = {
            **regexes,
            f'\\b(DOU|JULGO|DECLARO|JULGASE).*PREJUDICAD.*{obj}':
            'PREJUDICADO-RECURSO',
            f'\\b(DOU|DASE).*(SEGU|PROV)IMENTO.*{obj}':
            'PROVIMENTO-RECURSO',
            f'\\b(RESOLVO NEGAR|NEGO|NEGASE).*(SEGU|PROV)IMENTO.*{obj}':
            'IMPROVIMENTO-RECURSO',
            f'NAO (SE )?CONHEC.*{obj}':
            'NAO CONHECER-RECURSO',
        }        
    if classe in ["HC"]:
        obj = '(ORDEM|HABEAS|WRIT|IMPETRACAO|PEDIDO)'
        regexes = {
            **regexes,
            f'\\b(DEFIRO|CONCEDO).*{obj}':
            'CONCEDER-ORDEM',
            f'\\b(NEGO|DENEGO|INDEFIRO|INDEFERESE).*{obj}':
            'DENEGAR-ORDEM',
            'REJEITO.*INICIAL':
            'REJEITAR-INICIAL',
            f'NAO CONHEC.*{obj}':
            'NAO CONHECER-ORDEM',
            f'\\b(DOU|JULGO|DECLARO).*PREJUDICAD.*{obj}':
            'PREJUDICADA-ORDEM',
            'JULGO.*EXTINTO.*SEM.*MERITO':
            'S/MERITO-ORDEM',
        }
    if classe in ["RC"]: # Revisao Criminal
        obj = '(PEDIDO|REVISAO CRIMINAL|PLEITO|ACAO)'
        regexes = {
            **regexes,
            f'\\b(DEFIRO|CONCEDO|JULGO PROCEDENTE).*{obj}':
            'CONCEDER-PEDIDO',
            f'\\b(JULGO IMPROCEDENTE|NEGO|DENEGO|INDEFIRO|INDEFERESE|NAO ADMITO).*{obj}':
            'DENEGAR-PEDIDO',
            f'NAO (SE )?CONHEC.*{obj}':
            'NAO CONHECER-PEDIDO',
            'JULGO.*EXTINT.*SEM.*MERITO':
            'S/MERITO-ORDEM',
        }                
    if classe in ["APN"]:
        regexes = {
            **regexes,
            '(DECLAR|JULG|DECRET)[^.]*EXTIN[^.]*(PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO-',
            '(DECLAR|JULG|DECRET|PRONUNC).*PRESCRI':
            'PRESCRICAO-',
            'ACOLH.*DENUNCIA': 'ACOLHER-DENUNCIA',
            'CONDEN(O|AR|A-?L[AO])': 'CONDENAR',
            'ABSOLV(O|ER)': 'ABSOLVER',
        }
    if classe in ["Ap", "ApCrim", "ApCiv"]:
        for obj in ["APELACAO",
                    "APELO",
                    "APELOS",
                    "APELACOES",
                    "RECURSO",
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
            'NAO CONHEC.*RECURSO ESPECIAL':
            'NAO CONHECER-RECURSO ESPECIAL',
            '(NAO.{0,5}|IN)ADMIT.*RECURSO ESPECIAL':
            'NAO ADMITIR-RECURSO ESPECIAL',
            'ADMIT.*RECURSO ESPECIAL':
            'ADMITIR-RECURSO ESPECIAL',
        }
    if classe in ["RE"]:
        regexes = {
            **regexes,
            'NAO CONHEC.*RECURSO EXTRAORDINARIO':
            'NAO CONHECER-RECURSO EXTRAORDINARIO',
            '(NAO.{0,5}|IN)ADMIT.*RECURSO EXTRAORDINARIO':
            'NAO ADMITIR-RECURSO EXTRAORDINARIO',
            'ADMIT.*RECURSO EXTRAORDINARIO':
            'ADMITIR-RECURSO EXTRAORDINARIO',
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
            tipo_parte=None,
            classes=["ProOrd", "ACIA", "APN", "ED", "Ap"],
            replace_text={r'\.([0-9]{2})\b': r',\1'},
            remove_dots=[r'\barts?', r'\bfls?', '[0-9]', r'\bn', r'\bc', r'\bcc'],
            remove_regexes=[
                '(?s)(?i)conden[^.]{0,20}honor[^.]{0,10}adv',
                '(?s)(?i)conden[^.]{0,20}custa[^.]{0,10}proc',
            ],
            key_order=['ABSOLVO', 'PRESCRICAO', 'CONDENO'],
            name_match_single_parte=False,
            split_desfecho=True,
            main_sentence_regexes=get_main_sentence_regexes(),
            get_desfecho_regexes=get_desfecho_regexes,
            alternative_parte_regexes={'MINISTERIO.*PUBLICO': 'MP|MPF|MINISTERIO PUBLICO'},
            dispositivo_regexes=get_dispositivo_regexes(),
            all_partes_regexes=[r'\bOS REUS\b', r'\bOS REQUERIDOS\b', r'\bOS ACUSADOS'],
            more_regexes={
                'mode': get_mode(),
                'subject': get_subject()
            }
    ):
        self.text = _clean_text(text, replace_text, remove_dots, remove_regexes)
        self.main_sentence_regexes = main_sentence_regexes
        self.more_regexes = more_regexes
        self.all_partes_regexes = all_partes_regexes
        if parte is not None:
            ix = parte.index.intersection(text.index)
            self.parte = parte.loc[ix]
            self.text = self.text.loc[ix]
        self.tipo_parte = tipo_parte
        self.dispositivo_regexes = dispositivo_regexes
        self.alternative_parte_regexes = alternative_parte_regexes
        self.classes = classes
        self.text_pena_cols = ['regime', 'multa_unit']
        self.split_desfecho = split_desfecho
        self.key_order = key_order
        self.name_match_single_parte = name_match_single_parte
        self.get_desfecho_regexes = get_desfecho_regexes

    def parse(self):
        df = pd.DataFrame(index=self.text.index)
        df['main_sentence'] = _extract_regexes(
            self.text,
            self.main_sentence_regexes
        )
        df['main_sentence'] = clean_text(df.main_sentence)
        regexes = self.get_desfecho_regexes(self.classes)
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
        for k, v in self.more_regexes.items():
            df[k] = map_regex(df.main_sentence, v, keep_unmatched=False)
        self.parsed = df
        return df

    def parse_parte(self):
        self.dispositivo = clean_sentenca_text(
            _extract_regexes(
                self.text,
                self.dispositivo_regexes
            )
        )
        if self.name_match_single_parte:
            parsed_parte = self._parse_multiple_partes(self.dispositivo)
        else:
            single_parte = self.parte.groupby(self.parte.index.names).size() == 1
            parsed1 = self._parse_single_parte(self.dispositivo.loc[single_parte])
            parsed2 = self._parse_multiple_partes(self.dispositivo.loc[~single_parte])
            parsed_parte = pd.concat([parsed1, parsed2])
        parsed_parte = self._drop_duplicate_parte(parsed_parte)
        self.parsed_parte = parsed_parte
        return parsed_parte

    def _parse_single_parte(self, text):
        keys = get_split_keys(self.classes)
        df = pd.DataFrame({'text': text})
        df['key'] = map_regex(text, keys, keep_unmatched=False)
        df = df.query('key.notnull()')
        df = self._add_penas(df)
        df['parte'] = self.parte.loc[
            self.parte.index.drop_duplicates(keep=False)
        ]
        return df

    def _add_penas(self, df):
        regexes, boolean_regexes = get_pena_regexes(self.classes)
        if "APN" in self.classes:
            df['text'] = remove_pena_base(df.text)
        pena = extract_regexes(df.text, regexes, update=True)
        df = df.join(pena)
        num_cols = self._get_pena_cols(df, numeric_only=True)
        for c in num_cols:
            if c in df.columns:
                df[c] = extract_number(df[c])
                
        for k, v in boolean_regexes.items():
            df[k] = df.text.str.contains(v) * 1
        df = self._clean_penas(df)
        return df

    def _clean_penas(self, df):
        if intersect(["ACIA", "APN"], self.classes):
            pena_cols = self._get_pena_cols(df)
            for c in pena_cols:
                if c in df.columns:
                    df.loc[df.key != "CONDENO", c] = pd.NA
        if "APN" in self.classes:
            df.loc[df.regime.fillna('').str.contains('SEMI'), 'regime'] = "SEMIABERTO"
            df['prisao_dias'] = df.years.fillna(0)*365 + df.months.fillna(0)*30 + df.days.fillna(0)
        df = _fill_na(df)
        return df

    def _parse_multiple_partes(self, text):
        self.splitted = self._split_on_key(text)
        splitted = self._add_parte_regex(self.splitted)
        df = extractall_series(splitted.text, splitted.parte_regex)
        df = df.join(splitted.key, how='inner')
        df = self._add_penas(df)
        df = self._bfill_penas(df)
        df = self._add_all_partes(df)
        df = df.reset_index(['group', 'match'], drop=True)
        return df

    def _add_all_partes(self, df):
        # Changes OS REUS etc to all partes
        if self.all_partes_regexes is None:
            return df
        if type(self.all_partes_regexes) == list:
            return self._add_all_partes_sub(df)
        if type(self.all_partes_regexes) == dict:
            if self.tipo_parte is None:
                raise("Tipo parte must be specified")
            tipos_parte = self.tipo_parte.drop_duplicates().to_list()
            func = lambda x: self._add_all_partes_sub(df, x)
            return pd.concat(map(func, tipos_parte))

    def _add_all_partes_sub(self, df, tipo_parte=None):
        ix = df.index.names[0]
        df = df.reset_index()
        if tipo_parte is None:
            parte = self.parte.copy()
            all_parte_regex = '|'.join(self.all_partes_regexes)
        else:
            parte = self.parte.loc[self.tipo_parte==tipo_parte].copy()
            all_parte_regex = '|'.join(self.all_partes_regexes[tipo_parte])
        parte = parte.reset_index()
        has_all = df.parte.str.contains(all_parte_regex)
        pena_cols = self._get_pena_cols(df, numeric_only=True)
        all_partes = df.loc[has_all].drop(columns='parte')
        remaining = df.loc[~has_all]
        remaining['no_pena'] = df[pena_cols].sum(axis=1) == 0
        no_pena = (remaining.groupby(ix).no_pena.sum() > 0).reset_index()
        all_partes = all_partes.merge(no_pena, on=ix, how='left')
        all_partes['no_pena'] = all_partes.no_pena.fillna(True)
        all_partes = all_partes.query('no_pena==True')
        if len(all_partes) > 0:
            all_partes = all_partes.merge(parte, on=ix)
        df = pd.concat([
            df.loc[~has_all],
            all_partes
        ])
        df = df.drop(columns='no_pena')
        df = df.set_index([ix, 'group', 'match'])
        return df

    def _get_pena_cols(self, df, numeric_only=False):
        regexes, boolean_regexes = get_pena_regexes(self.classes)
        pena_cols = list(set(
            list(boolean_regexes.keys()) +
            pd.Series(regexes).str.extractall('<(.*?)>')[0].tolist()
        ))
        pena_cols = set(df.columns).intersection(pena_cols)
        if numeric_only:
            pena_cols = pena_cols - set(self.text_pena_cols)
        return pena_cols
    
    def _bfill_penas(self, df):
        # Backward fill penas in cases like:
        # CONDENO FRANCISCO SOARES E PEDRO SOUZA ...
        just_name = df.text.str.len() - df.parte.str.len() < 5
        condeno = df.key == 'CONDENO'
        pena_cols = self._get_pena_cols(df)
        df.loc[just_name & condeno, pena_cols] = pd.NA
        for col in pena_cols:
            df[col] = df[col].fillna(method='bfill')
        return df

    def _add_parte_regex(self, splitted):
        parte_regex = self._get_parte_regex()
        parte_regex = self._add_all_partes_regex(parte_regex)
        parte_regex = (
            r"(?s)(?P<text>\b(?P<parte>" + parte_regex + r")\b.*?)" +
            "(?=" + parte_regex + "|$)"
        )
        return splitted.join(parte_regex, how='inner')

    def _add_all_partes_regex(self, parte_regex):
        apr = self.all_partes_regexes
        if apr is None:
            return parte_regex
        if type(apr) == dict:
            apr = [j for i in apr.values() for j in i]
        all_partes = '|'.join(apr)
        return parte_regex + f'|{all_partes}'

    def _get_parte_regex(self):
        parte = clean_text(self.parte) + '|'
        if self.alternative_parte_regexes is not None:
            mapping = {k: f'{v}|' for k, v in self.alternative_parte_regexes.items()}
            mapped = map_regex(parte, mapping, keep_unmatched=False)
            parte = parte + mapped.fillna('')
        parte = parte.loc[parte.notnull()]
        regex = parte.groupby(parte.index.names).sum()
        regex = regex.str.replace(r"\|$", "", regex=True)
        regex.name = 'parte_regex'
        return regex
    
    def _split_on_key(self, text):
        # Splits on condeno, absolvo, etc
        keys = get_split_keys(self.classes)
        regex = r"(?i)\b(?P<key>{})\b".format('|'.join(keys.keys()))
        df = split_series(text, regex, drop_end=True)
        df['key'] = map_regex(df.key, keys)
        return df
    
    def _drop_duplicate_parte(self, df):
        pena_cols = self._get_pena_cols(df, numeric_only=True)
        df['penas'] = df[pena_cols].sum(axis=1)
        df['key_order'] = get_key_order(df.key, self.key_order)
        df['has_key'] = df.key.notnull() # Should not be necessary
        sort_cols = ['has_key', 'penas', 'key_order']
        ix_names = df.index.names
        df = (
            df
            .sort_values(sort_cols)
            .reset_index()
            .drop_duplicates(ix_names + ['parte'], keep='last')
            .set_index(ix_names)
            .drop(columns=['penas', 'key_order', 'has_key'])
        )
        return df

    def test(self, regex=None, max_str=2000, max_str_sentence=1000):
        if regex:
            text = self.text.loc[self.text.fillna('').str.contains(regex)]
        else:
            text = self.text
        sm = text.sample().index[0]
        parsed = self.parsed.loc[sm]
        try:
            print("TEXT:", self.text.loc[sm][-max_str:])
        except TypeError:
            print("No text")
            return 
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

    def test_parte(self, ix=None, regex=None, max_str=2000, max_str_dispositivo=1000, max_str_pena=1000):
        if regex:
            text = self.text.loc[self.text.fillna('').str.contains(regex)]
        else:
            text = self.text
        if ix:
            sm = ix
        else:
            sm = text.sample().index[0]
        try:
            print("TEXT:", self.text.loc[sm][-max_str:])
        except TypeError:
            print("No text")
            return 
        print('')
        print("DISPOSITIVO")
        print('')
        print_truncated(self.dispositivo.loc[sm], max_str_dispositivo)
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
                print_truncated(penas.text, max_str_pena)
                print(penas)
            else:
                for _, row in penas.iterrows():
                    print_truncated(row.text, max_str_pena)
                    print(row)
        except KeyError:
            print("Nothing extracted")
        return sm


def _fill_na(df):
    num_cols = df.select_dtypes(include='number').columns
    df[num_cols] = df[num_cols].fillna(0)
    obj_cols = df.select_dtypes(include='object').columns
    df[obj_cols] = df[obj_cols].fillna('')
    return df

def _extract_regexes(text, regexes):
    out = pd.Series(index=text.index, dtype=object, name=text.name)
    for regex in regexes:
        empty = out.isnull()
        out.loc[empty] = text.loc[empty].str.extract(regex)[0]
    return out
    

def print_truncated(string, max_str, first_share=0.9):
    if len(string) > max_str:
        print(string[:round(max_str*first_share)])
        print('...')
        print(string[-round(max_str*(1-first_share)):])
    else:
        print(string)


def intersect(list1, list2):
    return len(set(list1).intersection(list2)) > 0


def get_pena_regexes(classes=['APN', 'ACIA']):
    n = get_cardinal_number_regex().replace('(?i)(?s)', '')
    s = '[^.]{0,5}?'
    m = '[^.]{0,10}?'
    l = '[^.]{0,40}?'
    l2 = '[^.0-9]{0,40}?'
    years = f'(?P<years>{n}){l2}\\bANOS?\\b'
    months = f'(?P<months>{n}){l2}\\bMES(ES)?\\b'
    days = f'(?P<days>{n}){l2}\\bDIAS?\\b'
    tp = '(DETENCAO|RECLUSAO)'
    regexes = [
        f'MULTA[^.]+?\\bR[$\s]*(?P<multaR>{n})'
    ]
    boolean_regexes = {
        'has_multa': 'MULTA',
    }
    if intersect(['APN', 'ApCrim'], classes):
        regexes += [
            f'{months}{m}({days}{m})?{tp}',
            f'{years}{m}({months}{m}({days}{m})?)?{tp}',
            f'REGIME{l}(?P<regime>FECHADO|SEMI{s}ABERTO|ABERTO)',
            f'(?P<diasmulta>{n}){l2}DIAS{s}MULTA',
            f'DIAS{s}MULTA{m}(?P<diasmulta>{n})',
            f'PRESTACAO{s}SERVICOS{s}COMUNIDADE{l}(?P<comunidade>{n}){s}HORAS',
        ]
    if 'ACIA' in classes:
        regexes += [
            f'PROIB{m}CONTRATAR[^.]+?(?P<contratar>{n}){s}ANOS',
            f'DIREITOS{s}POLITICOS[^.]+?(?P<dirpol>{n}){s}ANOS',
            f'(RESSARCI|REPARACAO DO DANO|RECOLHIMENTO)[^.]+?\\bR[$\s]*(?P<ressarcirR>{n})',
            f'MULTA{l}(?P<multa>{n}){l}(?P<multa_unit>REMUNERACAO|DANO)',
        ]
        boolean_regexes = {
            **boolean_regexes,
            'perda_funcao': f'PERDA.{l}FUNCAO',        
            'ressarcir': 'RESSARCI|RESTITUICAO|REPARACAO DO DANO',
        }
    return regexes, boolean_regexes



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
            'CONDENO': 'CONDENO',
            'PARA CONDENAR': 'CONDENO',
            'JULGO PROCEDENTE': 'CONDENO',
            'JULGO IMPROCEDENTE': 'ABSOLVO',
            'PARA JULGAR PROCEDENTE': 'CONDENO',
            'DEIXO.{0,10}CONDENAR': 'ABSOLVO',
            'CONDEN(?:AR|A-?L[AO])': 'CONDENO',
            'ABSOLV(?:O|ER)': 'ABSOLVO',
        }
    if classe == "APN":
        keys = {
            **keys,
            'DOSIMETRIA': 'CONDENO',
            'CALCULO DA PENA': 'CONDENO',
            '(?:DECLARO|DECRETO|JULGO)[^.]*?EXTIN[^.]*?(?:PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO',
            '(?:JULGO|DECLARO|VERIFICO)[^.]*?PRESCRICAO': 'PRESCRICAO',
            'PRONUNCI(?:O|AR)': 'PRONUNCIO',
        }
    if classe == "Ap":
        keys = {
            **keys,
            'D(?:OU|AR) PARCIAL PROVIMENTO': 'PARCIAL PROVIMENTO',
            'D(?:OU|AR) PROVIMENTO PARCIAL': 'PARCIAL PROVIMENTO',
            'D(?:OU|AR) PROVIMENTO': 'PROVIMENTO',
            'NEG(?:O|AR) PROVIMENTO': 'IMPROVIMENTO',
        }
    return keys


def remove_pena_base(text):
    # Removing "FIXO PENA BASE DE 2 ANOS" etc
    # Important: Make sure \2 captures everything after (^.*)
    regex = '(?s)(^.*)(((TORN|FIX|FICA).{0,15}DEFINITIV[OA]|TOTALIZ(O|AM))[^.]+(DETENCAO|RECLUSAO))'
    return text.str.replace(regex, r'\2', regex=True) 


def get_df():
    df = pd.read_csv('../audit/build/clean/TRF1_inteiro_teor_text.csv')
    it = pd.read_csv('../audit/build/clean/TRF1_inteiro_teor.csv')
    df = df.merge(it, on=['num_npu', 'instancia', 'n_inteiro_teor'])
    #df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)condeno')]
    df = df.query('instancia==2')
    df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)apela..o')]
    #df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)deten[cç][aã]o|reclus[aã]o')]
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

#Destarte, fixo a pena-base no mínimo legal em 01 (um) ano de reclusão, a qual
#torno definitiva na ausência de circunstâncias atenuantes e agravantes, bem como causas de


#text = df.sample(100).inteiro_teor
#parser = DecisionParser(text, reu)
#pena = parser.parse_parte()
#sm = parser.test_parte()

# parser = DecisionParser(
#     text, parte, tipo_parte,
#     classes=["Ap", "ED", "ApCrim"],
#     dispositivo_regexes=[
#         r'(?i)(?s)(\b(?:nego|dou)\b.{0,10}provimento\b.*)',
#         '(?i)(?s)((?:pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto.*)',
#         '(?i)(?s)((?:diante|posto).{0,5}is[st]o.*)',
#     ],
#     main_sentence_regexes=[
#         r'(?i)(?s)(\b(?:nego|dou)\b.{0,10}provimento\b[^.]+)',
#         '(?i)(?s)((?:pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto[^.]+)',
#         '(?i)(?s)((?:diante|posto).{0,5}is[st]o[^.]+)',
#     ],
#     all_partes_regexes={
#         1: [r'\bD?OS? AUTOR(ES)?'],
#         2: [r'\bD?OS? REUS?', r'\bD?AS? RES?', r'\bD?[AO]S? ACUSAD[AO]S?', r'\bD?[AO]S? REQUERID[AO]S?']
#     },
#     more_regexes={
#         'voto_sentenca': {
#             'ANUL(AR|O)|NULIDADE': 'ANULAR',
#             'IMPROCEDENTE': 'IMPROCEDENTE',
#             'REFORMANDO IN TOTUM': 'IMPROCEDENTE',
#             'RETORN.{0,10}AUTOS|AUTOS.{0,10}RETORN': 'RETORNAR AUTOS',
#         }
#     }
# )
# pena = parser.parse_parte()
# sm = parser.test_parte()

if __name__ == '__main__':
    #df, parte = get_df()
    parser = DecisionParser(df.inteiro_teor, parte.parte)        
    out = parser.parse_parte()
    sm = parser.test_parte()

#out = parser.parse()
#sm = parser.test()


# DECIDE A TURMA NAO CONHECER DA REMESSA NECESSARIA E DA APELACAO DE VALDEVINO PEREIRA ROCHA; DAR PROVIMENTO A APELACAO DE ELDY FAGUNDES CAMELO E ELZITA FAGUNDES CAMELO ROCHA SILVA, E ESTENDER O RESULTADO ABSOLUTORIO AOS DEMANDADOS QUE NAO RECORRERAM E A VALDEVINO PEREIRA ROCHA ART 1005, PARAGRAFO UNICO - CPC, A UNANIMIDADE. 4A TURMA DO TRF DA 1A REGIAO - BRASILIA, BRASILIA, 26 DE JANEIRO DE 2021 DESEMBARGADOR FEDERAL OLINDO MENEZES, RELATOR
