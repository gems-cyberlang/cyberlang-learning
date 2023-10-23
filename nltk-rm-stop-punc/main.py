# OD removing stopwords and punc from text then creating freq dist with NLTK
# Check out https://realpython.com/python-nltk-sentiment-analysis/ for some details although not all
import nltk
from nltk.corpus import movie_reviews

nltk.download('punkt', 'movie_reviews', 'stop_words') # This is for the tokenization 

stopwords = nltk.corpus.stopwords.words("english")

r_id = movie_reviews.fileids()[0]

raw = movie_reviews.raw(r_id)
r_tokenized = nltk.word_tokenize(raw)

# Lowercase and remove punctuation
r_tokenized = [word.lower() for word in r_tokenized if word.isalpha()]
r_tokenized = [word for word in r_tokenized if word not in stopwords]

fd =  nltk.FreqDist(word for word in r_tokenized)
fd.plot()
