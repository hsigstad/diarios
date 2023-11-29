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


def get_dispositivo():
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
    return f'(?i)(?s)(?P<dispositivo>{regex})'


def get_mode():
    return {
        'UNANIMIDADE': 'UNANIMIDADE',
        'MAIORIA': 'MAIORIA',
    }
    

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
            split_desfecho=True,
            main_sentence_regexes=get_main_sentence_regexes(),
            dispositivo=get_dispositivo(),
            subject=get_subject(),
            mode=get_mode(),
    ):
        self.text = _clean_text(text, remove_dots)
        self.main_sentence_regexes = main_sentence_regexes
        self.subject = subject
        self.mode = mode
        self.parte = parte
        self.dispositivo = dispositivo
        self.classes = classes
        self.split_desfecho = split_desfecho


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
        single_parte = self.parte.groupby('num_npu').size() == 1
        parsed1 = self.parse_single_parte(self.text.loc[single_parte])
        parsed2 = self.parse_multiple_partes(self.text.loc[~single_parte])
        parsed_parte = pd.concat([parsed1, parsed2])
        parsed_parte = drop_duplicate_parte_mov(parsed_parte)
        self.parsed_parte = parsed_parte
        return parsed_parte

    def parse_single_parte(self, text):
        single_parte = self.parte.groupby('num_npu').size() == 1        
        text = remove_pena_base(text)
        df = pd.concat([text, self.parte.loc[single_parte]], axis=1)
        df = df.query('inteiro_teor.notnull()')
        df = df.rename(columns={'inteiro_teor': 'text'})
        dispositivo = clean_sentenca_text(
            text
            .str.extract(self.dispositivo)
            .dispositivo
        )
        self._dispositivo = dispositivo
        df['key'] = dispositivo.str.extract('(CONDENO|ABSOLVO)')
        df['text'] = dispositivo
        df = add_penas(df)
        return df

    def parse_multiple_partes(self, text):
        dispositivo = clean_sentenca_text(
            text
            .str.extract(self.dispositivo)
            .dispositivo
        )
        parte_regex = get_parte_regex(self.parte)
        parte_regex = r"(?s)(?P<text>\b(?P<parte>" + parte_regex + r")\b.*?)(?=" + parte_regex + "|$)"
        parte_regex.name = 'parte_regex'
        out = split_on_condeno_absolvo_etc(dispositivo)
        self._dispositivo = pd.concat([self._dispositivo, dispositivo])
        self.splitted = out
        out = out.join(parte_regex) # Removes those without parte
        out2 = extractall_series(out.dispositivo, out.parte_regex)
        out3 = out.join(out2)
        out3 = out3.query('key.notnull() & parte.notnull()')
        out3['text'] = remove_pena_base(out3.text)
        out3 = add_penas(out3)
        out3 = out3.reset_index(['group', 'match'], drop=True)
        return out3

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

    def test_parte(self, regex=None, max_str=2000, max_str_dispositivo=1000):
        if regex:
            text = self.text.loc[self.text.str.contains(regex)]
        else:
            text = self.text
        sm = text.sample().index[0]
        print(self.text.loc[sm][-max_str:])
        print('')
        print("DISPOSITIVO")
        print('')
        print(self._dispositivo.loc[sm][0:max_str_dispositivo])
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
                print(penas.text)
                print(penas)
            else:
                for _, row in self.parsed_parte.loc[sm].iterrows():
                    print(row.text)
                    print(row)
        except KeyError:
            print("Nothing extracted")
        return sm


def _clean_text(text, remove_dots):
    for r in remove_dots:
        text = text.str.replace(f'({r})\.', r'\1')
    return text


def get_parte_regex(parte):
    parte = clean_text(parte) + '|'
    parte = parte.loc[parte.notnull()]
    regex = parte.groupby(parte.index.name).sum()
    regex = regex.str.replace(r"\|$", "")
    return regex


def get_regexes(classes=['APN', 'ACIA']):
    n = get_cardinal_number_regex()
    s = '.{0,5}'
    m = '.{0,10}'
    l = '.{0,40}'
    years = f'(?P<years>{n}){m}\\bANOS?\\b'
    months = f'(?P<months>{n}){m}\\bMES(ES)?\\b'
    days = f'(?P<days>{n}){m}\\bDIAS?\\b'
    tp = '(DETENCAO|RECLUSAO)'
    regexes = [
        f'MULTA{m}\\bR\\b{s}(?P<multaR>{n})'
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
            multaR
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
    return regexes, boolean_regexes


def split_on_condeno_absolvo_etc(text):
    keys = {
        'CONDEN(?:O|AR)': 'CONDENO',
        'DOSIMETRIA': 'CONDENO',
        '(?:DECLARO|DECRETO|JULGO)[^.]*EXTIN[^.]*(?:PUNIBILIDADE|PUNITIVA)':
        'PRESCRICAO',
        '(?:JULGO|DECLARO|VERIFICO)[^.]*PRESCRICAO': 'PRESCRICAO',
        'ABSOLV(?:O|ER)': 'ABSOLVO',
        'PRONUNCI(?:O|AR)': 'PRONUNCIO',
    }
    df = split_series(text, r"(?i)\b(?P<key>{})\b".format('|'.join(keys.keys())))
    df['key'] = map_regex(df.key, keys)
    return df


def remove_pena_base(text):
    # Removing "FIXO PENA BASE DE 2 ANOS" etc
    return text.str.replace('(?s)(^.*)((TORN|FIX).{0,15}DEFINITIV[OA]|TOTALIZ(O|AM))', r'\2') 


def add_penas(df):
    regexes, boolean_regexes = get_regexes()
    out = extract_regexes(df.text, regexes, update=True)
    for c in ['years', 'months', 'days', 'diasmulta',
              'dirpol', 'contratar', 'multaR', 'ressarcirR']:
        if c in out.columns:
            out[c] = extract_number(out[c])
    for k, v in boolean_regexes.items():
        df[k] = df.text.str.contains(v)
    df2 = df.join(out)
    df2 = clean_penas(df2)
    return df2


def clean_penas(df2):
    df2['absolvido'] = df2.key == "ABSOLVO"
    df2['condenado'] = df2.key == "CONDENO"
    df2['prescricao'] = df2.key == "PRESCRICAO"
    cols = [
        'years',
        'months',
        'days',
        'diasmulta',
        'diasmulta2',
        'regime',
        'multaR',
        'ressarcir',
        'ressarcirR',
        'dirpol',
        'contratar',
    ]
    for c in cols:
        if c in df2.columns:
            df2.loc[df2.condenado==0, c] = pd.NA
    df2.loc[df2.regime.fillna('').str.contains('SEMI'), 'regime'] = "SEMIABERTO"
    df2['prisao_dias'] = df2.years.fillna(0)*365 + df2.months.fillna(0)*30 + df2.days.fillna(0)
    df2['diasmulta'] = df2.diasmulta.fillna(df2.diasmulta2)
    df2 = df2.drop(columns='diasmulta2')
    return df2


def drop_duplicate_parte_mov(df2):
    # TODO: Add dirpol, ressarcir etc here
    df2['has_pena'] = df2[['years', 'months', 'days', 'diasmulta']].notnull().sum(axis=1) > 0
    df2['key_order'] = 1 # PRESCRICAO has lowest priority
    # If condenado for something but absolvido for something else: condenado
    df2.loc[df2.key=="ABSOLVO", 'key_order'] = 2
    df2.loc[df2.key=="CONDENO", 'key_order'] = 3 
    df2['has_key'] = df2.key.notnull() # Should not be necessary
    sort_cols = ['has_key', 'has_pena', 'key_order']
    df4 = df2.sort_values(sort_cols).reset_index().drop_duplicates(
        ['num_npu', 'parte'],
        keep='last'
    ).set_index('num_npu')
    return df4

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

#df2, parte = get_df()

parser = DecisionParser(df2.inteiro_teor, parte.parte)        

out = parser.parse_parte()
sm = parser.test_parte()

safd

out = parser.parse()
sm = parser.test()




# IMPOE-SE, POIS, A ABSOLVICAO DOS ACUSADOS
# CONDENO O APENADO AO 
# DA RE



# mapping = {
#     "PARCIAL.{0,20}PROCEDENTE": "PARCIALMENTE PROCEDENTE",
#     "IMPROCEDENTE": "IMPROCEDENTE",
#     "PROCEDENTE.{0,20}PARTE": "PARCIALMENTE PROCEDENTE",
#     "PROCEDENTE": "PROCEDENTE",
#     "SEM.{0,20}MERITO": "S/MERITO",
#     "EXTINTA A PUNIBILIDADE": "PRESCRICAO",
#     "EXTINT": "EXTINTO",
#     "INCOMPETEN": "INCOMPETENCIA",
# }


# df["multa"] = df.sentenca_text.str.extract("MULTA[^.]*?R ?([0-9.,]{4,15})")
# df.loc[df.multa.isnull(), "multa"] = df.sentenca_text.str.extract(
#     "MULTA[^.]([0-9.,]{4,15})", expand=False
# )
# df["multa"] = pd.to_numeric(
#     df.multa.str.replace("\.[0-9]{1,2}[^0-9]*$", "")
#     .str.replace(",[0-9]{1,3}[^0-9]*$", "")
#     .str.replace("[,.]", "")
# )





#     '(?i)rejeito\s+os\s+embargos[^.]+declar': 'REJEITO EMBARGOS',
#     '(?i)dar\s+provimento\s+ao\s+recurso\s+em\s+sentido estrito': 'DAR PROVIMENTO AO RECURSO EM SENTIDO ESTRITO',
#     '(?i)dou\s+provimento[^.]+agravo\s+(regimental|interno)': 'DOU PROVIMENTO AO AGRAVO REGIMENTAL',
#     '(?i)negar\s+provimento[^.]+apel(ação|ações|o)': 'NEGAR PROVIMENTO A APELACAO',
#     '(?i)dar\s+parcial\s+provimento[^.]+apel(ação|ações|o)': 'DAR PARCIAL PROVIMENTO A APELACAO',
#     '(?i)dar\s+provimento[^.]+apel(ação|ações|o)': 'DAR PROVIMENTO A APELACAO',
#     '(?i)declaro[^.]+nulidade': 'DECLARO NULIDADE',
#     '(?i)julgo\s+prejudicada[^.]+apel(ação|ações|o)': 'JULGO PREJUDICADA A APELACAO',
#     '(?i)acompanho[^.]+voto[^.]+relator': 'ACOMPANHO O VOTO DO RELATOR',
#     '(?i)julgamento\s+adiado': 'JULGAMENTO ADIADO',
#     '(?i)retirado\s+d[ea]\s+pauta': 'RETIRADO DE PAUTA',
#     '(?i)processo\s+adiado': 'PROCESSO ADIADO',
#     '(?i)pediu\s+vista': 'PEDIU VISTA',    
# }

def get_text(sample=None):
    df = pd.read_csv('build/query/sentenca.csv')
    if sample is not None:
        df = df.sample(sample)
    df = df.drop_duplicates(['number', 'mov_id']) # 35 duplicates, not sure why
    df = df.set_index(['number', 'mov_id'])
    df['text'] = clean_sentenca_text(df.text)
    df['text'] = df.text.str.extract(r'(?s)(\b(JULGO|ABSOLVO|CONDENO)\b.*)')[0]
    return df.text






def test(df):
    sm = df.sample().iloc[0]
    print(text.loc[sm.number].iloc[0])
    print("-----------")
    print(sm.text)
    cols = [
        'parte', 'key',
        'years', 'months', 'days',
        'regime', 'diasmulta',
    ]
    print(sm[cols])
    return sm

# Most dia-multas are 1/30 of salario minimo:
# A RAZAO DE 130 UM TRINTA AVOS DO SALARIO-MINIMO DA EPOCA 
# SENDO CADA UM FIXADO NO VALOR EQUIVALENTE A UM TRIGESIMO DO SALARIO MINIMO
# QUE FIXO NA BASE DE UM TRIGESIMO DO SALARIO MINIMO VIGENTE
# A RAZAO DE 130 DO SALARIO MINIMO VIGENTE A EPOCA DOS FATOS


#text = get_text(sample=1000)
text = get_text()
df = split_on_condeno_absolvo_etc(text)
df = add_candidato(df)
df = keep_text_starting_with_parte_mention(df)
df['text'] = remove_pena_base(df.text)
df2 = add_penas(df)
df3 = df2.query('has_parte==True & key.notnull()')
df3 = drop_duplicate_parte_mov(df3)
cols = [
    "number",
    "mov_id",
    "parte",
    'condenado',
    'absolvido',
    'prescricao',
    'prisao_dias',
    "regime",
    'diasmulta',
]
df3.loc[:, cols].to_csv('build/clean/pena.csv', index=False)
sm = test(df3)


def test_dup(df):
    sm = df.query('dup>1 & has_key==True').sample().iloc[0]
    dd = df.query('number=="{}" & has_key==True'.format(sm.number))
    print(dd[['key', 'years', 'days', 'months', 'diasmulta', 'has_pena']])
    for t in dd.text:
        print(t)
    return sm
              
    
