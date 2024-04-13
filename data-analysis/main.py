from nltk import ngrams
import nltk
import string

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.tokenize.casual import casual_tokenize

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")
import pandas as pd
import csv

stop_words = set(stopwords.words("english"))

COMMENT_COLS = ["id", "subreddit", "body"]
df = pd.read_csv("sample-all.csv", sep=",", names=COMMENT_COLS)
df = df.reset_index()

wnl = WordNetLemmatizer()
n_grams_list = {}
for row in df.iterrows():
    body = row[1].body
    if type(body) == str:
        # TODO better bot detection?
        if "I am a bot" in body:
            continue

        if "traumatizing" in body:
            print(row[1].id, body)

        sent = []
        for token in casual_tokenize(
            body, preserve_case=False, reduce_len=True, strip_handles=False
        ):
            token = token.strip().lower()
            if token not in stop_words:
                if token not in string.punctuation:
                    if any(c.isalpha() for c in token): # Need at least one letter
                        sent.append(wnl.lemmatize(token))

        n_grams = nltk.ngrams(sent, 2)

        for n_g in n_grams:
            if n_g not in n_grams_list:
                n_grams_list[n_g] = []
            n_grams_list[n_g].append(row[1].id)

# filter = "trauma"
filter = None

with open("./out.csv", "w") as file:
    csv_out = csv.writer(file)

    for n_g in sorted(
        list(n_grams_list.keys()), key=lambda n_g: -len(n_grams_list[n_g])
    ):
        if filter:
            if all(filter not in w for w in n_g):
                continue
        ids = n_grams_list[n_g]
        csv_out.writerow([";".join(n_g), len(ids), ";".join(list(set(ids)))])
