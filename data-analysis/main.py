from nltk import ngrams
import nltk
import string

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
import pandas as pd
import csv

stop_words = set(stopwords.words('english'))

COMMENT_COLS = ["name", "subreddit","body"]
df = pd.read_csv("/home/odespo/com/projects/cyberlang-learning/data-analysis/sample-all.csv", sep=',', names=COMMENT_COLS)
df = df.reset_index()

wnl = WordNetLemmatizer()
n_grams_list = []
for row in df.iterrows():
    body = row[1].body
    if type(body) == str:
        for sentence in nltk.sent_tokenize(body):
            tokens = word_tokenize(sentence)
            sent = []
            for token in tokens:
                if token not in stop_words:
                    if token not in string.punctuation:
                        token = token.lower()
                        sent.append(wnl.lemmatize(token))
            
            n_grams = nltk.ngrams(sent, 2)

            for n_g in n_grams:
                n_grams_list.append(n_g) 

final_count = {}
for n_gram_name in set(n_grams_list):
    final_count[n_gram_name] = 0

for n_gram in n_grams_list:
    final_count[n_gram] = final_count[n_gram] + 1
file = open("./out.csv", "w")
csv_out = csv.writer(file)

for entrie in final_count.items():
    csv_out.writerow(entrie)
file.close()
