#!/usr/bin/env python

# taken from https://github.com/kavgan/nlp-in-practice/blob/master/word2vec/scripts/word2vec.py

import gzip
import logging
import os
import pandas as pd

# logging.basicConfig(
#     format='%(asctime)s : %(levelname)s : %(message)s',
#     level=logging.INFO)


# def show_file_contents(input_file):
#     with gzip.open(input_file, 'rb') as f:
#         for i, line in enumerate(f):
#             print(line)
#             break


# def read_input(input_file):
#     """This method reads the input file which is in gzip format"""

#     logging.info("reading file {0}...this may take a while".format(input_file))
#     with gzip.open(input_file, 'rb') as f:
#         for i, line in enumerate(f):

#             if (i % 10000 == 0):
#                 logging.info("read {0} reviews".format(i))
#             # do some pre-processing and return list of words for each review
#             # text
#             yield gensim.utils.simple_preprocess(line)


if __name__ == '__main__':

    abspath = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(abspath, "../sample-all-104005 - sample-all.csv")

    # load the csv
    df = pd.read_csv('/home/toh8473/cyberlang/cyberlang-learning/word-embeddings/word2vec-scripts/sample-all-104005 - sample-all.csv')

    # train w2v with csv
    sentences = [doc.split() for doc in df['text_column']]
    # model = Word2Vec(sentences, size=100, window=5, min_count=5, workers=4)

    # # read the tokenized reviews into a list
    # # each review item becomes a serries of words
    # # so this becomes a list of lists
    # documents = list(read_input(data_file))
    logging.info("Done reading data file")

    # build vocabulary and train model
    model = gensim.models.Word2Vec(
        sentences,
        size=150, # size of the vector that represents each token/word
        window=10, # window of similarity between target word and neighbor word
        min_count=2, # minimun frequency count of words, ignores anything below this
        workers=10) # number of threads that are working behind the scenes
    model.train(sentences, total_examples=len(sentences), epochs=10)

    # save only the word vectors
    model.wv.save(os.path.join(abspath, "../vectors/default"))

    w1 = "trauma"
    print("Most similar to {0}".format(w1), model.wv.most_similar(positive=w1))

    # # look up top 6 words similar to 'polite'
    # w1 = ["polite"]
    # print(
    #     "Most similar to {0}".format(w1),
    #     model.wv.most_similar(
    #         positive=w1,
    #         topn=6))

    # # look up top 6 words similar to 'france'
    # w1 = ["france"]
    # print(
    #     "Most similar to {0}".format(w1),
    #     model.wv.most_similar(
    #         positive=w1,
    #         topn=6))

    # # look up top 6 words similar to 'shocked'
    # w1 = ["shocked"]
    # print(
    #     "Most similar to {0}".format(w1),
    #     model.wv.most_similar(
    #         positive=w1,
    #         topn=6))

    # # look up top 6 words similar to 'shocked'
    # w1 = ["beautiful"]
    # print(
    #     "Most similar to {0}".format(w1),
    #     model.wv.most_similar(
    #         positive=w1,
    #         topn=6))

    # # get everything related to stuff on the bed
    # w1 = ["bed", 'sheet', 'pillow']
    # w2 = ['couch']
    # print(
    #     "Most similar to {0}".format(w1),
    #     model.wv.most_similar(
    #         positive=w1,
    #         negative=w2,
    #         topn=10))

    # # similarity between two different words
    # print("Similarity between 'dirty' and 'smelly'",
    #       model.wv.similarity(w1="dirty", w2="smelly"))

    # # similarity between two identical words
    # print("Similarity between 'dirty' and 'dirty'",
    #       model.wv.similarity(w1="dirty", w2="dirty"))

    # # similarity between two unrelated words
    # print("Similarity between 'dirty' and 'clean'",
    #       model.wv.similarity(w1="dirty", w2="clean"))