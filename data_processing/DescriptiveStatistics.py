import time
from textblob import TextBlob
import enchant
from pymongo import MongoClient
from nltk.tokenize import sent_tokenize, word_tokenize
import numpy
from nltk.corpus import stopwords
from nltk import BigramCollocationFinder
import nltk



bigram_measures = nltk.collocations.BigramAssocMeasures()

### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews
t0 = time.time()
companies = ['Apple']#glassdoor_reviews.distinct('company')


def stats():
    keys = {'pros':set(), 'cons':set(), 'advice_mgmt':set()}
    for company in companies:
        print(company)
        for review in glassdoor_reviews.find({'company':{'$eq':company}}):
            for key in keys.keys():
                if review[key] is not None:
                    val = len(word_tokenize(review[key]))
                    keys[key].append(val)

    for key in keys.keys():
        print(key)
        print("Average: " + str(numpy.average(keys[key])))
        print("Standard Deviation: " + str(numpy.std(keys[key])))
        print("Max: " + str(numpy.max(keys[key])))
        print("Min: " + str(numpy.min(keys[key])))

def bigrams():
    keys = {1: [], 2: [], 3: [], 4: [], 5: []}
    for key in keys.keys():
        for review in glassdoor_reviews.find({'rating': {'$eq': key}}):
            if review['pros'] is not None:
                keys[key].extend(word_tokenize(review['pros']))
            if review['cons'] is not None:
                keys[key].extend(word_tokenize(review['cons']))
            if review['advice_mgmt'] is not None:
                keys[key].extend(word_tokenize(review['advice_mgmt']))
        finder = BigramCollocationFinder.from_words(keys[key])
        ignored_words = nltk.corpus.stopwords.words('english')
        finder.apply_word_filter(lambda w: len(w) < 3 or w.lower() in ignored_words)
        # only bigrams that appear 3+ times
        finder.apply_freq_filter(3)
        # return the 10 n-grams with the highest PMI
        print('10 best n-grams for ' + str(key) + '-star')
        bigrams = finder.nbest(bigram_measures.likelihood_ratio, 10)
        print(nltk.FreqDist(bigrams).items())

def setOfWords():
    set_of_words = set()
    for idx, company in enumerate(companies):
        words = []
        print("Working on number: " + str(idx) + ", Name: " + company)
        print(company)
        for review in glassdoor_reviews.find({'company':{'$eq':company}}):
            if review['pros'] is not None:
                words.extend(word_tokenize(review['pros']))
            if review['cons'] is not None:
                words.extend(word_tokenize(review['cons']))
            if review['advice_mgmt'] is not None:
                words.extend(word_tokenize(review['advice_mgmt']))

            filtered = [w for w in words if w not in nltk.corpus.stopwords.words('english')]
            for l in filtered:
                set_of_words.add(l)
        print('Unique words' + str(len(set_of_words)))

    with open('output','w') as f:
        f.write("unique keys: " + str(len(set_of_words)))
        f.write(set_of_words)

def blobs():
    for idx, company in enumerate(companies):
        words = []
        print("Working on number: " + str(idx) + ", Name: " + company)
        print(company)
        for review in glassdoor_reviews.find({'company':{'$eq':company}}):
            if review['pros'] is not None:
                blob = TextBlob(review['pros'])
                for sentence in blob.sentences:
                    print(sentence)
                    print(sentence.sentiment.polarity)
            if review['cons'] is not None:
                blob = TextBlob(review['cons'])
                for sentence in blob.sentences:
                    print(sentence)
                    print(sentence.sentiment.polarity)
            #     words.extend(word_tokenize(review['pros']))
            # if review['cons'] is not None:
            #     words.extend(word_tokenize(review['cons']))
            # if review['advice_mgmt'] is not None:
            #     words.extend(word_tokenize(review['advice_mgmt']))


blobs()
    # if count > 99:
    #     total = keys['pros'] + keys['cons'] + keys['advice_mgmt']
    #     total_text = Text(total)
    #     fdist = FreqDist(total_text)
    #     val = sorted([w for w in set(total_text) if len(w) > 12 and w.isalpha() and not (d.check(w))])
    #     total_words += len(val)
    #     print(val)
    #     print("lenght of result: " + str(len(val)))
    #     keys = {'pros': [], 'cons': [], 'advice_mgmt': []}
    #     count = 0


#print("Total words: " + str(total_words))
#pro_text = Text(keys['pros'])
#con_text = Text(keys['cons'])
#mgmt_text = Text(keys['advice_mgmt'])

#print(total_text.collocations())
print ('Time taken: ' + str(time.time() - t0))






"""

Descriptive statistics needed

- 1 review date range
- parsing current/former employees
- average rating
- doesn't recommend/outlook/ceo


Data cleaning
- normalized by lowercasing, lemmatizing, and stoplisting.
- 1, 2, 3 gram attributes

"""

