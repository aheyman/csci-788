import time
import enchant
from pymongo import MongoClient
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk import Text
from nltk import FreqDist


d = enchant.Dict("en_US")
### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews
t0 = time.time()
companies = glassdoor_reviews.distinct('company')

keys = {'pros':[], 'cons':[], 'advice_mgmt':[]}
count = 0
total_words = 0
for company in companies:
    count += 1
    for review in glassdoor_reviews.find({'company':{'$eq':company}}):
        for key in keys.keys():
            if review[key] is not None:
                keys[key].extend(word_tokenize(review[key]))

    if count > 99:
        total = keys['pros'] + keys['cons'] + keys['advice_mgmt']
        total_text = Text(total)
        fdist = FreqDist(total_text)
        val = sorted([w for w in set(total_text) if len(w) > 12 and w.isalpha() and not (d.check(w))])
        total_words += len(val)
        print(val)
        print("lenght of result: " + str(len(val)))
        keys = {'pros': [], 'cons': [], 'advice_mgmt': []}
        count = 0

print("Total words: " + str(total_words))
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

