import os

from diarios import clean
import unittest
import numpy as np
import pandas as pd

# How to run tests:
# python -m pytest


class TestFunctions(unittest.TestCase):

    def test_map_regex(self):
        in_series = pd.Series(["abc", "aab", "xyz"], index=[5, 5, 2])
        mapping = {"ab": "yyy", "a": "xxx", "^xy": "zzz"}
        out_series = clean.map_regex(in_series, mapping)
        out_series2 = pd.Series(["yyy", "yyy", "zzz"], index=[5, 5, 2])
        self.assertTrue(all(out_series == out_series2))

    def test_convert_number_antigo(self):
        out1 = clean.convert_number_antigo(
            ["2016.51.01.164775-3", "660.01.2010.002107-1", "018.07.001979-4"],
            ["TRF2", "TJSP", "TJMS"],
        )
        out2 = pd.Series(
            [
                "0164775-04.2016.4.02.5101",
                "0002107-31.2010.8.26.0660",
                "0001979-89.2007.8.12.0018",
            ]
        )
        self.assertTrue(all(out1 == out2))

    def test_transform(self):
        mun = clean.transform(175, "municipio_id", "municipio")
        self.assertTrue(mun == "OURO PRETO DO OESTE")
        mun = clean.transform(175, ["municipio_id"], "municipio")
        self.assertTrue(mun == "OURO PRETO DO OESTE")
        sr1 = pd.Series([701, 7935])
        sr2 = pd.Series(["PARECIS", "GRAJAU"])
        mun = clean.transform(sr1, "municipio_id", "municipio")
        self.assertTrue(all(mun == sr2))
        mun = clean.transform(sr1, ["municipio_id"], "municipio")
        self.assertTrue(all(mun == sr2))
        df = pd.DataFrame({"x": [pd.NA, 1, 1, 1, pd.NA], "y": [4, 2, 2, 3, pd.NA]})
        infile = get_test_data_file("test_transform.csv")
        sr1 = clean.transform(df, from_var=["a", "b"], to_var="c", infile=infile)
        sr2 = pd.Series([pd.NA, 5, 5, 7, pd.NA])
        self.assertTrue(all(sr1.dropna() == sr2.dropna()))
        sr1 = clean.transform(df.y, from_var=["b"], to_var="c", infile=infile)
        sr2 = pd.Series([15, 5, 5, 7, pd.NA])
        self.assertTrue(all(sr1.dropna() == sr2.dropna()))


class TestCleanValor(unittest.TestCase):

    def test_basic(self):
        sr = pd.Series(["1.234,56", "100,00"])
        result = clean.clean_valor(sr)
        self.assertEqual(result.tolist(), ["1234.56", "100.00"])

    def test_no_decimals(self):
        sr = pd.Series(["1.000"])
        result = clean.clean_valor(sr)
        self.assertEqual(result.tolist(), ["1000"])


class TestCleanDate(unittest.TestCase):

    def test_yyyy_slash(self):
        result = clean.clean_date(pd.Series(["2021/03/15"]))
        self.assertEqual(result[0].tolist(), ["2021-03-15"])

    def test_yyyy_dash(self):
        result = clean.clean_date(pd.Series(["2020-01-15 extra text"]))
        self.assertEqual(result[0].tolist(), ["2020-01-15"])

    def test_null_and_empty(self):
        result = clean.clean_date(pd.Series([None, ""]))
        self.assertTrue(result[0].isna().all())


class TestCleanLine(unittest.TestCase):

    def test_numeric(self):
        result = clean.clean_line(pd.Series(["5", "10"]))
        self.assertEqual(result.tolist(), [5, 10])

    def test_non_numeric(self):
        result = clean.clean_line(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestRemoveLinks(unittest.TestCase):

    def test_markdown_link(self):
        sr = pd.Series(["[click here](http://example.com) text"])
        result = clean.remove_links(sr)
        self.assertEqual(result.tolist(), ["click here text"])

    def test_no_links(self):
        sr = pd.Series(["plain text"])
        result = clean.remove_links(sr)
        self.assertEqual(result.tolist(), ["plain text"])


class TestGetCapital(unittest.TestCase):

    def test_string(self):
        self.assertEqual(clean.get_capital("SP"), "SAO PAULO")
        self.assertEqual(clean.get_capital("RJ"), "RIO DE JANEIRO")

    def test_series(self):
        result = clean.get_capital(pd.Series(["MG", "BA"]))
        self.assertEqual(result.tolist(), ["BELO HORIZONTE", "SALVADOR"])


class TestCleanParteKey(unittest.TestCase):

    def test_removes_trailing_articles(self):
        result = clean.clean_parte_key(pd.Series(["autor aos", "reu a"]))
        self.assertEqual(result.tolist(), ["AUTOR", "REU"])

    def test_no_trailing_article(self):
        result = clean.clean_parte_key(pd.Series(["advogado"]))
        self.assertEqual(result.tolist(), ["ADVOGADO"])


class TestCleanComarca(unittest.TestCase):

    def test_uppercases(self):
        result = clean.clean_comarca(pd.Series(["recife"]))
        self.assertEqual(result.tolist(), ["RECIFE"])

    def test_removes_comarca_de_prefix(self):
        result = clean.clean_comarca(pd.Series(["comarca de recife"]))
        self.assertEqual(result.tolist(), ["RECIFE"])

    def test_removes_uppercase_comarca_de(self):
        result = clean.clean_comarca(pd.Series(["COMARCA DE SALVADOR"]))
        self.assertEqual(result.tolist(), ["SALVADOR"])


class TestCleanVara(unittest.TestCase):

    def test_extracts_number(self):
        result = clean.clean_vara(pd.Series(["1a vara civel", "3"]))
        self.assertEqual(result.tolist(), ["1", "3"])


class TestTitle(unittest.TestCase):

    def test_basic(self):
        result = clean.title(pd.Series(["SAO PAULO DE MINAS"]))
        self.assertEqual(result.tolist(), ["Sao Paulo de Minas"])

    def test_prepositions(self):
        result = clean.title(pd.Series(["RIO DE JANEIRO", "OURO PRETO DO OESTE"]))
        self.assertEqual(result.tolist(), ["Rio de Janeiro", "Ouro Preto do Oeste"])


class TestGetEstadoMapping(unittest.TestCase):

    def test_returns_27_states(self):
        m = clean.get_estado_mapping()
        self.assertEqual(len(m), 27)

    def test_known_entries(self):
        m = clean.get_estado_mapping()
        self.assertEqual(m["SAO PAULO"], "SP")
        self.assertEqual(m["BAHIA"], "BA")
        self.assertEqual(m["MINAS GERAIS"], "MG")


class TestGetDataFile(unittest.TestCase):

    def test_returns_existing_path(self):
        path = clean.get_data_file("municipio.csv")
        self.assertTrue(path.endswith(os.path.join("data", "municipio.csv")))
        self.assertTrue(os.path.exists(path))


class TestCleanCpf(unittest.TestCase):

    def test_numeric(self):
        result = clean.clean_cpf(pd.Series(["12345678901"]))
        self.assertEqual(result.tolist(), [12345678901])

    def test_as_str_pads(self):
        result = clean.clean_cpf(pd.Series(["123"]), as_str=True)
        self.assertEqual(result.tolist(), ["00000000123"])

    def test_invalid(self):
        result = clean.clean_cpf(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestCleanReais(unittest.TestCase):

    def test_with_decimals(self):
        result = clean.clean_reais(pd.Series(["1.234,56"]))
        self.assertEqual(result.tolist(), [1234])

    def test_currency_symbol(self):
        result = clean.clean_reais(pd.Series(["R$ 2.500,00"]))
        self.assertEqual(result.tolist(), [2500])

    def test_plain_number(self):
        result = clean.clean_reais(pd.Series(["300"]))
        self.assertEqual(result.tolist(), [300])

    def test_invalid(self):
        result = clean.clean_reais(pd.Series([""]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestCleanText(unittest.TestCase):

    def test_string_input(self):
        result = clean.clean_text("  Hello, World!  São Paulo  ")
        self.assertEqual(result, "HELLO WORLD SAO PAULO")

    def test_series_input(self):
        result = clean.clean_text(pd.Series(["café", None, "  hello  "]))
        self.assertEqual(result.tolist(), ["CAFE", "", "HELLO"])

    def test_returns_string_for_string_input(self):
        result = clean.clean_text("test")
        self.assertIsInstance(result, str)

    def test_returns_series_for_series_input(self):
        result = clean.clean_text(pd.Series(["test"]))
        self.assertIsInstance(result, pd.Series)

    def test_lower(self):
        result = clean.clean_text("Hello World", lower=True, upper=False)
        self.assertEqual(result, "hello world")

    def test_keep_accents(self):
        result = clean.clean_text("São Paulo", accents=True, drop=None)
        self.assertEqual(result, "SÃO PAULO")

    def test_custom_drop(self):
        result = clean.clean_text("abc 123", drop="^a-z ", upper=False)
        self.assertEqual(result, "abc")

    def test_newline_removed_by_default(self):
        result = clean.clean_text("hello\nworld")
        self.assertEqual(result, "HELLO WORLD")

    def test_newline_preserved(self):
        result = clean.clean_text("hello\nworld", newline=True, drop=None)
        self.assertIn("\n", result)

    def test_multiple_spaces_collapsed(self):
        result = clean.clean_text("hello    world")
        self.assertEqual(result, "HELLO WORLD")

    def test_multiple_spaces_preserved(self):
        result = clean.clean_text("hello    world", multiple_spaces=True)
        self.assertEqual(result, "HELLO    WORLD")

    def test_strip(self):
        result = clean.clean_text("  hello  ")
        self.assertEqual(result, "HELLO")

    def test_no_strip(self):
        result = clean.clean_text("  hello  ", strip=False)
        self.assertTrue(result.startswith(" "))


class TestCleanNumber(unittest.TestCase):

    def test_cnj_format(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.clean_number(sr)
        self.assertEqual(result.tolist(), ["0002107-31.2010.8.26.0660"])

    def test_strips_non_digits_around(self):
        sr = pd.Series(["Proc. 0002107-31.2010.8.26.0660 end"])
        result = clean.clean_number(sr)
        self.assertEqual(result.tolist(), ["0002107-31.2010.8.26.0660"])

    def test_no_digits(self):
        sr = pd.Series(["abc"])
        result = clean.clean_number(sr)
        self.assertEqual(result.iloc[0], "")


class TestIsCnjNumber(unittest.TestCase):

    def test_valid(self):
        result = clean.is_cnj_number(pd.Series(["0002107-31.2010.8.26.0660"]))
        self.assertTrue(result.iloc[0])

    def test_invalid(self):
        result = clean.is_cnj_number(pd.Series(["abc", "123"]))
        self.assertFalse(result.iloc[0])
        self.assertFalse(result.iloc[1])


class TestCleanEstado(unittest.TestCase):

    def test_full_names(self):
        result = clean.clean_estado(pd.Series(["São Paulo", "BAHIA"]))
        self.assertEqual(result.tolist(), ["SP", "BA"])

    def test_abbreviations_pass_through(self):
        result = clean.clean_estado(pd.Series(["SP", "RJ"]))
        self.assertEqual(result.tolist(), ["SP", "RJ"])

    def test_unknown(self):
        result = clean.clean_estado(pd.Series(["NEVERLAND"]))
        self.assertTrue(pd.isna(result.iloc[0]))


class TestCleanOab(unittest.TestCase):

    def test_series(self):
        result = clean.clean_oab(pd.Series(["OAB/SP 12345", "67890 RJ"]))
        self.assertEqual(result.tolist(), ["12345/SP", "67890/RJ"])

    def test_string_input(self):
        result = clean.clean_oab("12345 SP")
        self.assertEqual(result.tolist(), ["12345/SP"])


class TestCleanParte(unittest.TestCase):

    def test_removes_titles_and_suffixes(self):
        result = clean.clean_parte(pd.Series(["DRS JOAO SILVA E OUTROS"]))
        self.assertEqual(result.tolist(), ["JOAO SILVA"])

    def test_maps_mp(self):
        result = clean.clean_parte(pd.Series(["MINISTERIO PUBLICO"]))
        self.assertEqual(result.tolist(), ["MP"])

    def test_delete_pattern(self):
        result = clean.clean_parte(
            pd.Series(["JOAO", "SECRET INFO"]),
            delete="SECRET",
        )
        self.assertEqual(result.iloc[0], "JOAO")
        self.assertEqual(result.iloc[1], "")


class TestCleanClasse(unittest.TestCase):

    def test_known_classes(self):
        result = clean.clean_classe(pd.Series([
            "ACAO CIVIL PUBLICA",
            "AGRAVO DE INSTRUMENTO",
            "APELACAO",
        ]))
        self.assertEqual(result.tolist(), ["ACP", "AI", "Ap"])

    def test_unmatched_kept(self):
        result = clean.clean_classe(pd.Series(["xyz"]))
        self.assertEqual(result.tolist(), ["XYZ"])


class TestCleanTipoParte(unittest.TestCase):

    def test_plaintiff_types(self):
        result = clean.clean_tipo_parte(pd.Series(["AUTOR", "REQUERENT"]))
        self.assertEqual(result.tolist(), ["PLAINTIFF", "PLAINTIFF"])

    def test_defendant_types(self):
        result = clean.clean_tipo_parte(pd.Series(["REU", "REQUERID"]))
        self.assertEqual(result.tolist(), ["DEFENDANT", "DEFENDANT"])

    def test_lawyer(self):
        result = clean.clean_tipo_parte(pd.Series(["ADVOGADO"]))
        self.assertEqual(result.tolist(), ["LAWYER"])


class TestCleanDecision(unittest.TestCase):

    def test_grau_1(self):
        result = clean.clean_decision(pd.Series([
            "JULGO PROCEDENTE",
            "JULGO IMPROCEDENTE",
            "JULGO PARCIALMENTE PROCEDENTE",
            "HOMOLOGO O ACORDO",
        ]))
        self.assertEqual(result.tolist(), [
            "PROCEDENTE",
            "IMPROCEDENTE",
            "PARCIALMENTE PROCEDENTE",
            "HOMOLOGO ACORDO",
        ])

    def test_grau_2(self):
        result = clean.clean_decision(
            pd.Series(["DERAM PROVIMENTO", "NEGAR PROVIMENTO"]),
            grau="2",
        )
        self.assertEqual(result.tolist(), ["DERAM", "NEGAR"])

    def test_unmatched_kept(self):
        result = clean.clean_decision(pd.Series(["random text"]))
        self.assertEqual(result.tolist(), ["RANDOM TEXT"])


class TestGetPlaintiffwinsMapping(unittest.TestCase):

    def test_default(self):
        m = clean.get_plaintiffwins_mapping()
        self.assertEqual(m["PROCEDENTE"], 1)
        self.assertEqual(m["IMPROCEDENTE"], 0)
        self.assertEqual(m["PARCIALMENTE PROCEDENTE"], 1)

    def test_parcial_zero(self):
        m = clean.get_plaintiffwins_mapping(parcial=0)
        self.assertEqual(m["PARCIALMENTE PROCEDENTE"], 0)


class TestGetFilingYear(unittest.TestCase):

    def test_cnj_number(self):
        result = clean.get_filing_year(pd.Series(["0002107-31.2010.8.26.0660"]))
        self.assertEqual(result.tolist(), [2010])


class TestCleanInteger(unittest.TestCase):

    def test_digit_string(self):
        result = clean.clean_integer(pd.Series(["42"]))
        self.assertEqual(result.tolist(), [42])

    def test_number_words(self):
        result = clean.clean_integer(pd.Series(["CINCO", "DEZ"]))
        self.assertEqual(result.tolist(), [5, 10])

    def test_invalid(self):
        result = clean.clean_integer(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestExtractFromList(unittest.TestCase):

    def test_first_match_wins(self):
        sr = pd.Series(["JULGO PROCEDENTE a acao", "HOMOLOGO ACORDO"])
        regex_list = ["JULGO .{0,20}PROCEDENTE", "HOMOLOGO .{0,20}ACORDO"]
        result = clean.extract_from_list(sr, regex_list)
        self.assertEqual(result.iloc[0], "JULGO PROCEDENTE")
        self.assertEqual(result.iloc[1], "HOMOLOGO ACORDO")

    def test_no_match(self):
        sr = pd.Series(["nothing here"])
        result = clean.extract_from_list(sr, ["PATTERN"])
        self.assertTrue(pd.isna(result.iloc[0]))


class TestCleanCnjNumber(unittest.TestCase):

    def test_already_clean(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.clean_cnj_number(sr)
        self.assertEqual(result.iloc[0], "0002107-31.2010.8.26.0660")

    def test_pads_short_number(self):
        sr = pd.Series(["2107-31.2010.8.26.0660"])
        result = clean.clean_cnj_number(sr)
        self.assertEqual(result.iloc[0], "0002107-31.2010.8.26.0660")

    def test_coerce_invalid(self):
        sr = pd.Series(["abc"])
        result = clean.clean_cnj_number(sr, errors="coerce")
        self.assertTrue(pd.isna(result.iloc[0]))

    def test_ignore_invalid(self):
        sr = pd.Series(["abc"])
        result = clean.clean_cnj_number(sr, errors="ignore")
        self.assertEqual(result.iloc[0], "abc")


class TestExtractSeries(unittest.TestCase):

    def test_named_groups(self):
        text = pd.Series(["case 123 year 2020", "number 456 year 2019"])
        regex = r"(?P<num>\d+) year (?P<year>\d+)"
        result = clean.extract_series(text, regex)
        self.assertEqual(result.columns.tolist(), ["num", "year"])
        self.assertEqual(result["num"].tolist(), ["123", "456"])
        self.assertEqual(result["year"].tolist(), ["2020", "2019"])

    def test_per_row_regex(self):
        text = pd.Series(["abc 123", "xyz 456"])
        regex = pd.Series([r"(?P<val>\d+)", r"xyz (?P<val>\d+)"])
        result = clean.extract_series(text, regex)
        self.assertEqual(result["val"].tolist(), ["123", "456"])

    def test_no_match(self):
        text = pd.Series(["no match"])
        regex = r"(?P<num>\d+)"
        result = clean.extract_series(text, regex)
        self.assertEqual(len(result), 1)


class TestExtractallSeries(unittest.TestCase):

    def test_multiple_matches(self):
        text = pd.Series(["a1 b2 c3"], index=[10])
        regex = r"(?P<letter>[a-z])(?P<digit>\d)"
        result = clean.extractall_series(text, regex)
        self.assertEqual(len(result), 3)
        self.assertEqual(result["letter"].tolist(), ["a", "b", "c"])
        self.assertEqual(result["digit"].tolist(), ["1", "2", "3"])

    def test_index_names(self):
        text = pd.Series(["a1 b2"])
        result = clean.extractall_series(text, r"(?P<x>[a-z])(?P<y>\d)")
        self.assertEqual(result.index.names[-1], "match")

    def test_custom_level_name(self):
        text = pd.Series(["a1"])
        result = clean.extractall_series(text, r"(?P<x>\w+)", level_name="item")
        self.assertEqual(result.index.names[-1], "item")


class TestSplitSeries(unittest.TestCase):

    def test_basic_split(self):
        text = pd.Series(["A: hello B: world"], name="txt")
        regex = r"(?P<key>[A-Z]):\s*"
        result = clean.split_series(text, regex, text_pos="right", drop_end=True)
        self.assertIn("key", result.columns)
        self.assertIn("txt", result.columns)
        self.assertEqual(result["key"].tolist(), ["A", "B"])

    def test_raises_on_series_regex(self):
        text = pd.Series(["hello"])
        regex = pd.Series(["world"])
        with self.assertRaises(TypeError):
            clean.split_series(text, regex)


class TestExtractNumber(unittest.TestCase):

    def test_numeric(self):
        result = clean.extract_number(pd.Series(["42"]))
        self.assertEqual(result.iloc[0], 42)

    def test_decimal(self):
        result = clean.extract_number(pd.Series(["3,14"]))
        self.assertAlmostEqual(result.iloc[0], 3.14)

    def test_cardinal_word(self):
        result = clean.extract_number(pd.Series(["CINCO"]))
        self.assertEqual(result.iloc[0], 5)

    def test_ordinal_word(self):
        result = clean.extract_number(pd.Series(["TERCEIRA"]))
        self.assertEqual(result.iloc[0], 3)

    def test_no_match(self):
        result = clean.extract_number(pd.Series(["nada"]))
        self.assertTrue(pd.isna(result.iloc[0]))

    def test_numeric_only(self):
        result = clean.extract_number(pd.Series(["42"]), cardinal=False, ordinal=False)
        self.assertEqual(result.iloc[0], 42)

    def test_words_only(self):
        result = clean.extract_number(pd.Series(["VINTE"]), numeric=False)
        self.assertEqual(result.iloc[0], 20)


class TestNormalizeDatajud(unittest.TestCase):

    def _make_record(self, pid, numero, classe_codigo=7, assuntos=None):
        return {
            "_id": pid,
            "_source": {
                "numeroProcesso": numero,
                "tribunal": "TJSP",
                "grau": "G1",
                "nivelSigilo": 0,
                "classe": {"codigo": classe_codigo, "nome": "Procedimento Comum"},
                "orgaoJulgador": {"codigo": 100, "nome": "Vara X"},
                "assuntos": assuntos or [{"codigo": 10, "nome": "Assunto A"}],
            },
        }

    def test_returns_expected_keys(self):
        records = [self._make_record("p1", "0002107-31.2010.8.26.0660")]
        result = clean.normalize_datajud(records)
        self.assertEqual(
            sorted(result.keys()),
            ["assuntos", "classes", "orgaos_julgadores", "processo_assuntos", "processos"],
        )

    def test_processos_shape(self):
        records = [
            self._make_record("p1", "0002107-31.2010.8.26.0660"),
            self._make_record("p2", "0001234-56.2015.8.26.0100"),
        ]
        result = clean.normalize_datajud(records)
        self.assertEqual(len(result["processos"]), 2)
        self.assertIn("numero_processo", result["processos"].columns)

    def test_assuntos_bridge(self):
        records = [
            self._make_record("p1", "0002107-31.2010.8.26.0660", assuntos=[
                {"codigo": 10, "nome": "A"},
                {"codigo": 20, "nome": "B"},
            ]),
        ]
        result = clean.normalize_datajud(records)
        self.assertEqual(len(result["processo_assuntos"]), 2)
        self.assertEqual(len(result["assuntos"]), 2)

    def test_nested_assuntos(self):
        records = [{
            "_id": "p1",
            "_source": {
                "numeroProcesso": "0002107-31.2010.8.26.0660",
                "classe": {"codigo": 1},
                "orgaoJulgador": {},
                "assuntos": [[{"codigo": 10, "nome": "A"}, {"codigo": 20, "nome": "B"}]],
            },
        }]
        result = clean.normalize_datajud(records)
        self.assertEqual(len(result["processo_assuntos"]), 2)

    def test_classe_filter(self):
        records = [
            self._make_record("p1", "0002107-31.2010.8.26.0660", classe_codigo=7),
            self._make_record("p2", "0001234-56.2015.8.26.0100", classe_codigo=99),
        ]
        result = clean.normalize_datajud(records, classe_codigos=[7])
        self.assertEqual(len(result["processos"]), 1)

    def test_empty_records(self):
        result = clean.normalize_datajud([])
        self.assertEqual(len(result["processos"]), 0)


class TestGenerateId(unittest.TestCase):

    def test_series(self):
        sr = pd.Series(["a", "b", "a", "c"])
        result = clean.generate_id(sr)
        self.assertEqual(result.iloc[0], result.iloc[2])
        self.assertNotEqual(result.iloc[0], result.iloc[1])

    def test_with_suffix(self):
        sr = pd.Series(["a", "b"])
        result = clean.generate_id(sr, suffix=1)
        self.assertTrue(all(r % 100 == 1 for r in result))

    def test_dataframe(self):
        df = pd.DataFrame({"x": ["a", "b", "a"], "y": ["1", "2", "1"]})
        result = clean.generate_id(df, by=["x", "y"])
        self.assertEqual(result.iloc[0], result.iloc[2])
        self.assertNotEqual(result.iloc[0], result.iloc[1])


class TestRemoveRegexes(unittest.TestCase):

    def test_removes_patterns(self):
        sr = pd.Series(["hello WORLD 123 foo"])
        result = clean.remove_regexes(sr, ["WORLD", "123"])
        self.assertEqual(result.iloc[0].strip(), "hello   foo")

    def test_empty_list(self):
        sr = pd.Series(["unchanged"])
        result = clean.remove_regexes(sr, [])
        self.assertEqual(result.iloc[0], "unchanged")


class TestMapRegexEdgeCases(unittest.TestCase):

    def test_string_match(self):
        self.assertEqual(clean.map_regex("abc", {"ab": "X"}), "X")

    def test_string_no_match_keep(self):
        self.assertEqual(clean.map_regex("xyz", {"ab": "X"}), "xyz")

    def test_string_no_match_drop(self):
        result = clean.map_regex("xyz", {"ab": "X"}, keep_unmatched=False)
        self.assertTrue(np.isnan(result))

    def test_nan_input(self):
        result = clean.map_regex(np.NaN, {"ab": "X"})
        self.assertTrue(np.isnan(result))

    def test_ndarray_input(self):
        arr = np.array(["abc", "xyz"])
        result = clean.map_regex(arr, {"ab": "Y"})
        self.assertEqual(result.tolist(), ["Y", "xyz"])


class TestCleanTextColumns(unittest.TestCase):

    def test_cleans_object_columns(self):
        df = pd.DataFrame({"a": ["São Paulo"], "b": [1], "c": ["café"]})
        result = clean.clean_text_columns(df)
        self.assertEqual(result["a"].iloc[0], "SAO PAULO")
        self.assertEqual(result["c"].iloc[0], "CAFE")
        self.assertEqual(result["b"].iloc[0], 1)

    def test_exclude(self):
        df = pd.DataFrame({"a": ["São Paulo"], "b": ["café"]})
        result = clean.clean_text_columns(df, exclude=["b"])
        self.assertEqual(result["a"].iloc[0], "SAO PAULO")
        self.assertEqual(result["b"].iloc[0], "café")


class TestGetData(unittest.TestCase):

    def test_returns_dataframe(self):
        df = clean.get_data("municipio_id.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)


class TestMoveColumnsFirst(unittest.TestCase):

    def test_reorders(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = clean.move_columns_first(df, ["c", "b"])
        self.assertEqual(result.columns.tolist(), ["c", "b", "a"])

    def test_missing_column_ignored(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = clean.move_columns_first(df, ["z", "b"])
        self.assertEqual(result.columns[0], "b")


class TestGetVerificadorCnj(unittest.TestCase):

    def test_known_value(self):
        result = clean.get_verificador_cnj("0002107", "201082600660")
        self.assertEqual(result, "79")

    def test_invalid_returns_none(self):
        result = clean.get_verificador_cnj("abc", "xyz")
        self.assertIsNone(result)


class TestAddLeadsAndLags(unittest.TestCase):

    def test_creates_lag_and_lead_columns(self):
        df = pd.DataFrame({
            "id": [1, 1, 1],
            "year": [2000, 2001, 2002],
            "value": [10, 20, 30],
        })
        result = clean.add_leads_and_lags(df, ["value"], "id", "year", [1, -1])
        self.assertIn("value1", result.columns)
        self.assertIn("value-1", result.columns)

    def test_lag_values(self):
        df = pd.DataFrame({
            "id": [1, 1, 1],
            "year": [2000, 2001, 2002],
            "value": [10, 20, 30],
        })
        result = clean.add_leads_and_lags(df, ["value"], "id", "year", [1])
        mid = result[result.year == 2001]
        self.assertEqual(mid["value1"].iloc[0], 30)

    def test_boundary_nan(self):
        df = pd.DataFrame({
            "id": [1, 1],
            "year": [2000, 2001],
            "value": [10, 20],
        })
        result = clean.add_leads_and_lags(df, ["value"], "id", "year", [1])
        last = result[result.year == 2001]
        self.assertTrue(np.isnan(last["value1"].iloc[0]))


class TestCleanDiarioText(unittest.TestCase):

    def test_preserves_accents_and_newlines(self):
        result = clean.clean_diario_text(pd.Series(["São Paulo\ncafé 123!"]))
        self.assertIn("São Paulo", result.iloc[0])
        self.assertIn("\n", result.iloc[0])
        self.assertIn("café", result.iloc[0])


class TestGetNumberRegex(unittest.TestCase):

    def test_cnj(self):
        regex = clean.get_number_regex("CNJ")
        self.assertIsInstance(regex, str)
        self.assertIn("filingyear", regex)

    def test_tjsp(self):
        regex = clean.get_number_regex("TJSP")
        self.assertIn("filingyear", regex)


class TestGetNumberRegexes(unittest.TestCase):

    def test_returns_dict(self):
        regexes = clean.get_number_regexes()
        self.assertIsInstance(regexes, dict)
        self.assertIn("CNJ", regexes)
        self.assertGreater(len(regexes), 10)


class TestGetIntegerMapping(unittest.TestCase):

    def test_returns_dict(self):
        m = clean.get_integer_mapping()
        self.assertEqual(m["UMA?"], "1")
        self.assertEqual(m["DEZ"], "10")
        self.assertEqual(m["CEM"], "100")


class TestGetOrdinalNumberRegex(unittest.TestCase):

    def test_returns_string(self):
        regex = clean.get_ordinal_number_regex()
        self.assertIsInstance(regex, str)
        self.assertIn("PRIMEIR", regex)


class TestGetCardinalNumberRegex(unittest.TestCase):

    def test_returns_string(self):
        regex = clean.get_cardinal_number_regex()
        self.assertIsInstance(regex, str)
        self.assertIn("[0-9]", regex)


class TestTRT(unittest.TestCase):

    def test_from_int(self):
        trt = clean.TRT(5)
        self.assertEqual(trt.name, "TRT5")
        self.assertEqual(trt.n, 5)
        self.assertIsInstance(trt.estados, list)

    def test_from_str(self):
        trt = clean.TRT("TRT5")
        self.assertEqual(trt.name, "TRT5")
        self.assertEqual(trt.n, 5)


class TestTRF(unittest.TestCase):

    def test_from_int(self):
        trf = clean.TRF(1)
        self.assertEqual(trf.name, "TRF1")
        self.assertEqual(trf.n, 1)
        self.assertIsInstance(trf.estados, list)
        self.assertGreater(len(trf.estados), 0)

    def test_from_str(self):
        trf = clean.TRF("TRF1")
        self.assertEqual(trf.name, "TRF1")
        self.assertEqual(trf.n, 1)


class TestGetTrtEstados(unittest.TestCase):

    def test_returns_list(self):
        result = clean.get_trt_estados("TRT1")
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)


class TestGetTrfEstados(unittest.TestCase):

    def test_returns_list(self):
        result = clean.get_trf_estados("TRF1")
        self.assertIsInstance(result, list)
        self.assertIn("AC", result)


class TestGetTrfEstadosMapping(unittest.TestCase):

    def test_all_five_trfs(self):
        m = clean.get_trf_estados_mapping()
        self.assertEqual(len(m), 5)
        for key in ["TRF1", "TRF2", "TRF3", "TRF4", "TRF5"]:
            self.assertIn(key, m)
            self.assertIsInstance(m[key], list)


class TestGetPlaintiffwins(unittest.TestCase):

    def test_maps_decisions(self):
        sr = pd.Series(["PROCEDENTE", "IMPROCEDENTE", "PARCIALMENTE PROCEDENTE"])
        result = clean.get_plaintiffwins(sr)
        self.assertEqual(result.tolist(), [1, 0, 1])

    def test_parcial_zero(self):
        sr = pd.Series(["PARCIALMENTE PROCEDENTE"])
        result = clean.get_plaintiffwins(sr, parcial=0)
        self.assertEqual(result.tolist(), [0])


class TestGetDecision(unittest.TestCase):

    def test_grau_1(self):
        sr = pd.Series(["JULGO PROCEDENTE a acao", "HOMOLOGO O ACORDO"])
        result = clean.get_decision(sr)
        self.assertIn("JULGO PROCEDENTE", result.iloc[0])
        self.assertIn("HOMOLOGO", result.iloc[1])

    def test_grau_2(self):
        sr = pd.Series(["DERAM PROVIMENTO AO RECURSO V. U."])
        result = clean.get_decision(sr, grau="2")
        self.assertIn("DERAM", result.iloc[0])

    def test_no_match(self):
        sr = pd.Series(["nothing relevant"])
        result = clean.get_decision(sr)
        self.assertTrue(pd.isna(result.iloc[0]))


class TestGetProcedencia(unittest.TestCase):

    def test_procedente(self):
        sr = pd.Series(["julgo procedente o pedido"])
        result = clean.get_procedencia(sr)
        self.assertEqual(result.iloc[0], "PROCEDENTE")

    def test_parcialmente(self):
        sr = pd.Series(["julgo parcialmente procedente"])
        result = clean.get_procedencia(sr)
        self.assertEqual(result.iloc[0], "PARCIALMENTE PROCEDENTE")


class TestCleanLei(unittest.TestCase):

    def test_lei(self):
        result = clean.clean_lei(pd.Series(["lei 8666/1993"]))
        self.assertEqual(result.iloc[0], "L8666/93")

    def test_decreto_lei(self):
        result = clean.clean_lei(pd.Series(["decreto-lei 1234/2001"]))
        self.assertEqual(result.iloc[0], "DL1234/01")

    def test_lei_complementar(self):
        result = clean.clean_lei(pd.Series(["lei complementar 101/2000"]))
        self.assertEqual(result.iloc[0], "LC101/00")

    def test_no_number_unchanged(self):
        result = clean.clean_lei(pd.Series(["CPC"]))
        self.assertEqual(result.iloc[0], "CPC")


class TestExtractInfoFromCaseNumbers(unittest.TestCase):

    def test_cnj(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.extract_info_from_case_numbers(sr)
        self.assertEqual(result["filingyear"].iloc[0], 2010)
        self.assertEqual(result["code_j"].iloc[0], 8)
        self.assertEqual(result["code_tr"].iloc[0], 26)
        self.assertEqual(result["oooo"].iloc[0], 660)


class TestGetTribunal(unittest.TestCase):

    def test_from_number(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.get_tribunal(sr, input_type="number")
        self.assertEqual(result.iloc[0], "TJSP")

    def test_from_diario(self):
        sr = pd.Series(["DJAC"])
        result = clean.get_tribunal(sr, input_type="diario")
        self.assertEqual(result.iloc[0], "TJAC")


class TestIsNumberAntigo(unittest.TestCase):

    def test_old_format_detected(self):
        sr_num = pd.Series(["660.01.2010.002107-1"])
        sr_trib = pd.Series(["TJSP"])
        result = clean.is_number_antigo(sr_num, sr_trib)
        self.assertTrue(result.iloc[0])

    def test_cnj_not_detected(self):
        sr_num = pd.Series(["0002107-31.2010.8.26.0660"])
        sr_trib = pd.Series(["TJSP"])
        result = clean.is_number_antigo(sr_num, sr_trib)
        self.assertFalse(result.iloc[0])


class TestCleanNumberAntigo(unittest.TestCase):

    def test_trf2(self):
        sr_num = pd.Series(["2016.51.01.164775-3"])
        sr_trib = pd.Series(["TRF2"])
        result = clean.clean_number_antigo(sr_num, sr_trib)
        self.assertEqual(result.iloc[0], "2016.51.01.164775-3")


class TestGetOldFormat(unittest.TestCase):

    def test_filters_non_cnj(self):
        df = pd.DataFrame({"num": [
            "0002107-31.2010.8.26.0660",
            "660.01.2010.002107-1",
        ]})
        result = clean.get_old_format(df, "num")
        self.assertEqual(len(result), 1)


class TestCleanMunicipio(unittest.TestCase):

    def test_string(self):
        result = clean.clean_municipio("SAO PAULO", "SP")
        self.assertEqual(result, "SAO PAULO")

    def test_series(self):
        result = clean.clean_municipio(pd.Series(["SAO PAULO"]), "SP")
        self.assertEqual(result.iloc[0], "SAO PAULO")


class TestExtractMunicipio(unittest.TestCase):

    def test_string(self):
        result = clean.extract_municipio("mora em SAO PAULO capital", "SP")
        self.assertEqual(result, "SAO PAULO")

    def test_series(self):
        sr = pd.Series(["mora em SAO PAULO capital"])
        result = clean.extract_municipio(sr, "SP")
        self.assertEqual(result.iloc[0], "SAO PAULO")


class TestGetMunicipioId(unittest.TestCase):

    def test_known_municipio(self):
        sr_mun = pd.Series(["SAO PAULO"])
        sr_est = pd.Series(["SP"])
        result = clean.get_municipio_id(sr_mun, sr_est)
        self.assertFalse(pd.isna(result.iloc[0]))


class TestGetMunicipioRegex(unittest.TestCase):

    def test_returns_regex_string(self):
        regex = clean.get_municipio_regex("SP")
        self.assertIsInstance(regex, str)
        self.assertGreater(len(regex), 100)

    def test_matches_known_city(self):
        import re
        regex = clean.get_municipio_regex("SP")
        self.assertIsNotNone(re.search(regex, "SAO PAULO"))


class TestGetFofoInfo(unittest.TestCase):

    def test_returns_dataframe(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.get_foro_info(sr)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertIn("tribunal", result.columns)


class TestGetForo(unittest.TestCase):

    def test_returns_value(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.get_foro(sr)
        self.assertFalse(pd.isna(result.iloc[0]))


class TestGetComarcaId(unittest.TestCase):

    def test_from_comarca_tribunal(self):
        result = clean.get_comarca_id(
            comarca=pd.Series(["SAO PAULO"]),
            tribunal=pd.Series(["TJSP"]),
        )
        self.assertFalse(pd.isna(result.iloc[0]))

    def test_raises_without_args(self):
        with self.assertRaises(Exception):
            clean.get_comarca_id()


class TestGetCadernoId(unittest.TestCase):

    def test_known_caderno(self):
        diario = pd.Series(["CNJ"])
        caderno = pd.Series(["edicao extra"])
        result = clean.get_caderno_id(diario, caderno)
        self.assertEqual(result.iloc[0], 1)


class TestLoadDatajudJsonl(unittest.TestCase):

    def test_reads_jsonl(self):
        import tempfile
        import json
        records = [{"_id": "1", "data": "test"}, {"_id": "2", "data": "test2"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
            path = f.name
        result = clean.load_datajud_jsonl(path)
        os.unlink(path)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["_id"], "1")


class TestGetDecisaoId(unittest.TestCase):

    def test_maps_known_decisions(self):
        s = pd.Series(["PROCEDENTE", "IMPROCEDENTE"])
        result = clean.get_decisao_id(s)
        self.assertEqual(result.tolist(), [1, 2])

    def test_unknown_returns_nan(self):
        s = pd.Series(["NONEXISTENT"])
        result = clean.get_decisao_id(s)
        self.assertTrue(pd.isna(result.iloc[0]))


class TestGetTipoParteId(unittest.TestCase):

    def test_maps_known_types(self):
        s = pd.Series(["PLAINTIFF", "DEFENDANT"])
        result = clean.get_tipo_parte_id(s)
        self.assertEqual(result.tolist(), [1, 2])

    def test_unknown_returns_nan(self):
        s = pd.Series(["UNKNOWN"])
        result = clean.get_tipo_parte_id(s)
        self.assertTrue(pd.isna(result.iloc[0]))


class TestGetForoId(unittest.TestCase):

    def test_returns_foro_column(self):
        s = pd.Series(["0164775-04.2016.4.02.5101"])
        result = clean.get_foro_id(s)
        self.assertEqual(result.iloc[0], 4025101)


class TestGetComarca(unittest.TestCase):

    def test_returns_comarca_name(self):
        s = pd.Series(["0164775-04.2016.4.02.5101"])
        result = clean.get_comarca(s)
        self.assertIsInstance(result.iloc[0], str)
        self.assertGreater(len(result.iloc[0]), 0)


def get_test_data(datafile):
    infile = get_test_data_file(datafile)
    return pd.read_csv(infile)


def get_test_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, "data", datafile)


if __name__ == "__main__":
    unittest.main()
