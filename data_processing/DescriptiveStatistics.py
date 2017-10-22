import csv
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from random import randint
import traceback
import math
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords


### DB connections
client = MongoClient()
glassdoor_db = client.test2
glassdoor_reviews = glassdoor_db.reviews

companies = glassdoor_reviews.distinct('company')

for company in companies:
    reviews = glassdoor_reviews.find({'company':{'$eq':company}})
    for review in reviews:
        data = review["pros"]
        stopWords = set(stopwords.words('english'))
        words = word_tokenize(data)
        wordsFiltered = []

for w in words:
    if w not in stopWords:
        wordsFiltered.append(w)

print(wordsFiltered)



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

