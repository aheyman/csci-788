import enchant
from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import datetime
import re
from nltk import Text
from nltk.tokenize import sent_tokenize, word_tokenize
from wordsegment import load, segment

def enum_caster(x):
    return {
        'Recommends': 1,
        'Doesn\'t Recommend': -1,
        'Positive Outlook':1,
        'Neutral Outlook':0,
        'Negative Outlook':-1,
        'Approves of CEO':1,
        'No opinion of CEO':0,
        'Disapproves of CEO':-1
    }[x]

def xstr(s):
    if s is None:
        return ''
    return str(s)

count = 0

load() #word segement
d = enchant.Dict("en_US")

### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews

companies = glassdoor_reviews.distinct('company')
lowercase_fields = ['review_title','job_title','duration','pros','cons','advice_mgmt']
enumerate_fields = ['recommend', 'outlook', 'ceo']
pattern = re.compile(r"\((\d+)\)") # parsing out the helpful values

for idx, company in enumerate(companies):
    print("Working on number: " + str(idx) + ", Name: " + company)
    operations = []

    for doc in glassdoor_reviews.find({'company':{'$eq':company}}
                                      ):
        # # Lowercase
        # for field in lowercase_fields:
        #     if doc[field] is not None:
        #         operations.append(
        #             UpdateOne({"_id": doc["_id"]}, {"$set": {field: doc[field].lower()}})
        #         )
        #
        # # dupes in Duration
        # if doc['duration'] == doc['pros']:
        #     operations.append(
        #              UpdateOne({"_id": doc["_id"]}, {"$set": {'duration': None}})
        #          )

        # If the review contains 3 or more long, misspelled words, segment
        words = Text(word_tokenize(xstr(doc.get('pros')) + xstr(doc.get('cons')) + xstr(doc.get('advice_mgmt'))))
        if len([w for w in set(words) if len(w) > 12 and w.isalpha() and not (d.check(w))]) > 2:
            pros = ' '.join(word for word in segment(xstr(doc['pros'])))
            cons = ' '.join(word[0] for word in segment(xstr(doc['cons'])))
            mgmt = ' '.join(word[0] for word in segment(xstr(doc['advice_mgmt'])))
            operations.extend([
                UpdateOne({"_id": doc["_id"]}, {"$set": {'pros': pros}}),
                UpdateOne({"_id": doc["_id"]}, {"$set": {'cons': cons}}),
                UpdateOne({"_id": doc["_id"]}, {"$set": {'advice_mgmt': mgmt}})
                ]
            )
        #
        # # convert enums
        # for enum in enumerate_fields:
        #     if doc[enum] is not None:
        #         val = enum_caster(doc[enum])
        #         operations.append(
        #             UpdateOne({"_id": doc["_id"]}, {"$set": {enum: val}})
        #         )
        #
        # # convert to date
        # if doc['review_date'] is not None:
        #     date = datetime.datetime.strptime(doc['review_date'],'%Y-%m-%d')
        #     operations.append(
        #         UpdateOne({"_id": doc["_id"]}, {"$set": {'review_date': date}})
        #     )
        #
        # # pase out helpful as a numerical value
        # if doc['helpful'] is not None:
        #     val = pattern.findall(doc['helpful'])[0]
        #     operations.append(
        #         UpdateOne({"_id": doc["_id"]}, {"$set": {'helpful': int(val)}})
        #     )

        # Send once every 1000 in batch
        if (len(operations) == 1000):
            glassdoor_reviews.bulk_write(operations, ordered=False)
            operations = []

    if (len(operations) > 0):
        glassdoor_reviews.bulk_write(operations, ordered=False)



