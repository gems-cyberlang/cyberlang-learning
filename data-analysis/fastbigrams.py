# FAST BIRGAMSSSS ZOOOOOOOOOOOOOOOOOOOOOOOOOOM
# got to install sklearn
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd

COMMENT_COLS = ["id", "subreddit", "body"]
FILE = "sample-all.csv"

def run_fast_bigrams(file, save=False):
    df = pd.read_csv(FILE, sep=",", names=COMMENT_COLS)
    df = df[df['body'].notna()]

    ngram_vectorizer = CountVectorizer(ngram_range=(2, 2)) # Count all bigrams
    count_matrix = ngram_vectorizer.fit_transform(df["body"])
    bigram_counts = count_matrix.sum(axis=0)

    feature_names = ngram_vectorizer.get_feature_names_out()
    df_bigram_counts = pd.DataFrame({'Bigram': feature_names, 'Count': bigram_counts.A1})

    df_bigram_counts_sorted = df_bigram_counts.sort_values(by='Count', ascending=False)
    if save: df_bigram_counts_sorted.to_csv("out_put_run1.csv", index=False)

if(__name__ == "__main__"):
    df_birgams = run_fast_bigrams(FILE, save=True)