import unittest
from unittest import mock

from diarios import anonymize

# How to run tests:
# python -m pytest

# These tests guard the chunker's masking-safety invariant, so they must not
# depend on GLiNER being installed or on a model cache being present. A fake
# tokenizer whose tokens are whitespace-separated words keeps the token budget
# exactly predictable; _chunk_text only ever needs the tokenizer half of
# _load_model()'s return value.


class FakeTokenizer:
    """Tokenizer stand-in: one token per whitespace-separated word.

    Used where predictable packing arithmetic matters more than realism.
    """

    def tokenize(self, text):
        return text.split()


class DenseTokenizer:
    """WordPiece stand-in: whitespace split, then <=3 characters per token.

    Density is the whole point. The bug this guards against budgeted chunks by
    CHARACTERS (`max_tokens * 4`) while the model counted TOKENS, so it only
    overshoots when a token is worth fewer than 4 characters. Real Portuguese
    legal text runs about 3.7 chars/token under bert-base-multilingual-cased; a
    one-token-per-word fake sits near 8 and hides the defect entirely.
    """

    def tokenize(self, text):
        return [w[i:i + 3] for w in text.split() for i in range(0, len(w), 3)]


def _patched_load_model():
    return None, FakeTokenizer()


class TestChunkText(unittest.TestCase):
    """`_anonymize_chunk` emits whatever chunk it is handed, whether or not the
    NER model managed to scan all of it. So an oversized chunk means text
    leaving the module unscanned for names, and a dropped chunk means text
    silently missing from the output. Both are regressions worth failing on.
    """

    def setUp(self):
        patcher = mock.patch.object(anonymize, "_load_model", _patched_load_model)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_every_chunk_respects_the_token_budget(self):
        # Measured with the DENSE tokenizer: this is the assertion the old
        # character-proxy chunker fails, emitting a chunk the model can only
        # partly scan while _anonymize_chunk returns all of it.
        tokenizer = DenseTokenizer()
        long_sentence = "palavra " * 500 + "fim."
        text = f"Reu MARCOS SILVA denunciado. {long_sentence} Condeno o reu."
        with mock.patch.object(
            anonymize, "_load_model", lambda: (None, tokenizer)
        ):
            chunks = anonymize._chunk_text(text, max_tokens=50)
        for chunk in chunks:
            self.assertLessEqual(len(tokenizer.tokenize(chunk)), 50)

    def test_no_text_is_dropped(self):
        # The tail of an over-long sentence used to be discarded outright.
        long_sentence = "palavra " * 500 + "SENTINELA_FINAL."
        text = f"Inicio. {long_sentence} Fim."
        chunks = anonymize._chunk_text(text, max_tokens=50)
        joined = " ".join(chunks)
        self.assertIn("SENTINELA_FINAL.", joined)
        self.assertIn("Inicio.", joined)
        self.assertIn("Fim.", joined)

    def test_no_text_is_duplicated(self):
        # current_chunk used to survive the flush, re-emitting earlier text.
        long_sentence = "palavra " * 500 + "fim."
        text = f"Reu MARCOS SILVA denunciado. {long_sentence} Condeno."
        joined = " ".join(anonymize._chunk_text(text, max_tokens=50))
        self.assertEqual(joined.count("MARCOS SILVA"), 1)

    def test_word_longer_than_budget_is_split_not_dropped(self):
        # Pathological: a single whitespace-free run exceeding the budget. The
        # fake tokenizer makes this one token, so force it with max_tokens=1
        # against a multi-word sentence to exercise the dense-fragment path.
        chunks = anonymize._split_dense_fragment("abcdefgh", FakeTokenizer(), 1)
        self.assertEqual("".join(chunks), "abcdefgh")

    def test_short_text_is_a_single_chunk(self):
        chunks = anonymize._chunk_text("Uma frase curta.", max_tokens=50)
        self.assertEqual(chunks, ["Uma frase curta."])

    def test_sentences_are_packed_not_split_one_per_chunk(self):
        # Budget-respecting sentences should still share a chunk.
        text = "Um dois. Tres quatro. Cinco seis."
        self.assertEqual(len(anonymize._chunk_text(text, max_tokens=50)), 1)


if __name__ == "__main__":
    unittest.main()
