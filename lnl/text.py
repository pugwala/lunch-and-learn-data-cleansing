"""Sessions 6-7: searching free text for 'good conduct' and measuring how well each approach works."""
import importlib
import re

import numpy as np
import pandas as pd

from . import config, env

# ---- Session 6 vocabulary (the finished version; the notebook builds it up step by step) ------
GOOD_WORDS = {"good", "well", "great", "excellent", "outstanding", "fine", "cooperative", "respectful",
              "polite", "courteous", "compliant", "exemplary", "stellar", "solid", "positive", "commendable"}
GOOD_PHRASES = ["no issues", "no problems", "no incidents", "no trouble", "squared away", "model inmate",
                "pleasure to supervise", "zero write-ups", "without complaint", "10/10", "a+",
                "stayed out of trouble", "helped staff", "volunteered", "de-escalated"]
NEGATORS = {"not", "no", "never", "n't", "hardly", "nothing"}
NEGATING_PHRASES = ["anything but", "far from", "no longer"]
EXCLUDE_TOKENS = {"goods", "goodman", "goode"}           # words that contain "good" but aren't praise
EXCLUDE_PHRASES = ["good time", "good conduct time"]     # TDCJ sentence-credit terms, not behavior


# ---- Data -----------------------------------------------------------------------------------
def sample_notes(df: pd.DataFrame, n: int = None, seed: int = 7) -> pd.DataFrame:
    """Notes plus their true label, sampled so everyone works on the same rows."""
    from . import pipeline
    key = pipeline.answer_key()
    base = df.reset_index() if df.index.name == "tdcj_number" else df
    notes = base[["tdcj_number", "conduct_notes"]].dropna().merge(key, on="tdcj_number")
    notes = notes[notes["conduct_label"] != "no_note"]
    n = min(n or config.TEXT_SAMPLE_SIZE, len(notes))
    notes = notes.sample(n=n, random_state=seed).reset_index(drop=True)
    notes["is_good"] = notes["conduct_label"] == "positive"
    return notes.rename(columns={"conduct_notes": "note", "conduct_label": "true_label"})


# ---- Scoring --------------------------------------------------------------------------------
def score(predicted, truth) -> dict:
    predicted = np.asarray(predicted, dtype=bool)
    truth = np.asarray(truth, dtype=bool)
    tp = int((predicted & truth).sum())
    fp = int((predicted & ~truth).sum())
    fn = int((~predicted & truth).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"returned": tp + fp, "correct (TP)": tp, "wrong (FP)": fp, "missed (FN)": fn,
            "precision": round(precision, 3), "recall": round(recall, 3), "F1": round(f1, 3)}


class Scoreboard:
    """Keeps every attempt side by side so progress (or regress) is obvious."""

    def __init__(self, truth):
        self.truth = np.asarray(truth, dtype=bool)
        self.rows = {}

    def add(self, name: str, predicted) -> pd.DataFrame:
        self.rows[name] = score(predicted, self.truth)
        return pd.DataFrame.from_dict({name: self.rows[name]}, orient="index")

    def show(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.rows, orient="index")

    def plot(self):
        import matplotlib.pyplot as plt
        table = self.show()[["precision", "recall", "F1"]]
        ax = table.plot.barh(figsize=(8, 0.5 * len(table) + 1.5), xlim=(0, 1))
        ax.invert_yaxis()
        ax.set_xlabel("score (1.0 = perfect)")
        ax.legend(loc="lower right")
        plt.tight_layout()
        return ax


def mistakes(notes: pd.DataFrame, predicted, kind: str = "wrong", n: int = 8) -> pd.DataFrame:
    """kind='wrong' shows false positives; kind='missed' shows false negatives."""
    predicted = np.asarray(predicted, dtype=bool)
    mask = (predicted & ~notes["is_good"]) if kind == "wrong" else (~predicted & notes["is_good"])
    return notes.loc[mask, ["tdcj_number", "note", "true_label"]].set_index("tdcj_number").head(n)


# ---- spaCy ----------------------------------------------------------------------------------
def load_spacy():
    """English pipeline with only the parts we need (fast). Downloads the model on first use."""
    env.ensure("spacy")
    import spacy
    try:
        return spacy.load("en_core_web_sm", disable=["parser", "ner"])
    except OSError:
        print("Downloading the small English model (one-time)...")
        from spacy.cli import download
        download("en_core_web_sm")
        importlib.invalidate_caches()
        return spacy.load("en_core_web_sm", disable=["parser", "ner"])


def tokenize(notes: pd.Series, nlp) -> list:
    """For each note: a list of (lowercase word, lemma) pairs."""
    return [[(t.lower_, t.lemma_.lower()) for t in doc if not t.is_space]
            for doc in nlp.pipe(notes.str.lower().tolist(), batch_size=500)]


def _negated(words: list, position: int, window: int) -> bool:
    if window <= 0:
        return False
    before = words[max(0, position - window):position]
    if any(w in NEGATORS for w in before):
        return True
    text_before = " ".join(before)
    return any(p in text_before for p in NEGATING_PHRASES)


def lexicon_search(notes: pd.Series, tokens: list, words=GOOD_WORDS, phrases=GOOD_PHRASES,
                   negation_window: int = 0, exclude: bool = False) -> np.ndarray:
    """True where a note contains a 'good' word (by lemma) or phrase, optionally skipping negated
    matches and known look-alikes."""
    results = []
    for text, pairs in zip(notes.str.lower().tolist(), tokens):
        surface = [w for w, _ in pairs]
        if exclude:
            for phrase in EXCLUDE_PHRASES:
                text = text.replace(phrase, " ")
        found = False
        for i, (word, lemma) in enumerate(pairs):
            if exclude and (word in EXCLUDE_TOKENS or " ".join(surface[i:i + 2]) in EXCLUDE_PHRASES
                            or " ".join(surface[i:i + 3]) in EXCLUDE_PHRASES):
                continue
            if lemma in words and not _negated(surface, i, negation_window):
                found = True
                break
        if not found:
            for phrase in phrases:
                start = text.find(phrase)
                if start >= 0:
                    words_before = text[:start].split()
                    if not _negated(words_before, len(words_before), negation_window):
                        found = True
                        break
        results.append(found)
    return np.array(results)


def best_rules(notes: pd.Series, tokens: list) -> np.ndarray:
    """Session 6's finished rule set: lexicon + phrases + negation + exclusions."""
    return lexicon_search(notes, tokens, negation_window=3, exclude=True)


# ---- Session 7: embeddings --------------------------------------------------------------------
class Embedder:
    """Turns text into vectors of meaning. Uses a small sentence-transformer model when available;
    otherwise falls back to Latent Semantic Analysis (TF-IDF + SVD), which runs anywhere."""

    def __init__(self, model_name: str = None):
        model_name = model_name or config.EMBEDDING_MODEL
        self.method = "lsa"
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(model_name)
            self.method = f"sentence-transformers ({model_name})"
        except Exception as exc:  # not installed, or the model can't be downloaded here
            print(f"Sentence-transformer model unavailable ({type(exc).__name__}); using Latent Semantic "
                  "Analysis instead. It matches overlapping words more than meaning, so expect weaker results. "
                  "See EMBEDDING_MODEL in lnl/config.py.")
            from sklearn.decomposition import TruncatedSVD
            from sklearn.feature_extraction.text import TfidfVectorizer
            self.vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=2, sublinear_tf=True)
            self.svd = TruncatedSVD(n_components=150, random_state=0)
        print(f"Embedding method: {self.method}")

    def _normalize(self, matrix):
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        return matrix / np.where(norms == 0, 1, norms)

    def fit_encode(self, texts) -> np.ndarray:
        texts = list(texts)
        if self.method == "lsa":
            return self._normalize(self.svd.fit_transform(self.vectorizer.fit_transform(texts)))
        return self.model.encode(texts, batch_size=128, normalize_embeddings=True, show_progress_bar=True)

    def encode(self, texts) -> np.ndarray:
        texts = list(texts)
        if self.method == "lsa":
            return self._normalize(self.svd.transform(self.vectorizer.transform(texts)))
        return self.model.encode(texts, normalize_embeddings=True)


def semantic_search(embedder: Embedder, matrix: np.ndarray, notes: pd.DataFrame, query: str,
                    k: int = 10) -> pd.DataFrame:
    """Top-k notes closest in meaning to the query (duplicate wordings shown once)."""
    scores = matrix @ embedder.encode([query])[0]
    ranked = pd.DataFrame({"similarity": scores.round(3), "note": notes["note"].values,
                           "true_label": notes["true_label"].values}).sort_values("similarity", ascending=False)
    return ranked.drop_duplicates("note").head(k).reset_index(drop=True)


def topic_table(texts, n_topics: int = 6, extra_stop_words=(), top_words: int = 8, seed: int = 0):
    """TF-IDF + NMF topic model. Returns (topics table, topic number per note)."""
    from sklearn.decomposition import NMF
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer
    stop = list(ENGLISH_STOP_WORDS.union(w.lower() for w in extra_stop_words))
    vectorizer = TfidfVectorizer(stop_words=stop, min_df=5, max_df=0.5, token_pattern=r"(?u)\b[a-z][a-z]+\b")
    matrix = vectorizer.fit_transform([t.lower() for t in texts])
    model = NMF(n_components=n_topics, random_state=seed, init="nndsvda", max_iter=400)
    weights = model.fit_transform(matrix)
    vocab = np.array(vectorizer.get_feature_names_out())
    rows = {f"Topic {i + 1}": ", ".join(vocab[np.argsort(-comp)[:top_words]])
            for i, comp in enumerate(model.components_)}
    assignment = weights.argmax(axis=1) + 1
    table = pd.DataFrame({"top words": rows})
    table["notes"] = pd.Series(assignment).value_counts().reindex(range(1, n_topics + 1)).values
    return table, assignment
