from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import datetime
from textblob import TextBlob
import re
import csv
from nltk import Text

### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews

quarters = [1,2,3,4]
years = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]
file = 'enums.csv'
header = ['Company', 'Quarter', 'Year', 'Recomendation', 'Outlook', 'Ceo']

companies = glassdoor_reviews.distinct('company')
lowercase_fields = ['review_title','job_title','duration','pros','cons','advice_mgmt']
enumerate_fields = ['recommend', 'outlook', 'ceo']
pattern = re.compile(r"\((\d+)\)") # parsing out the helpful values

results = []
with open(file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)

try:
    for idx, company in enumerate(companies):
        print("Working on number: " + str(idx) + ", Name: " + company)
        for year in years:
            for quarter in quarters:
                sentiment = []
                enums = {'recommend':[], 'outlook':[], 'ceo':[]}
                for doc in glassdoor_reviews.find({'$and':[
                                                    {'company':{'$eq':company}},
                                                    {'quarter':{'$eq':quarter}},
                                                    {'review_date':{'$gte':datetime.datetime(year,1,1),
                                                                    '$lt':datetime.datetime(year+1,1,1)}
                                                    }]}):
                    # result = doc['pros'] + doc['cons']
                    # tb = TextBlob(result)
                    # sentiment.append(tb.sentiment.polarity)

                    for enum in enums.keys():
                        if doc[enum] is not None:
                            enums[enum].append(doc[enum])




                if len(sentiment) > 0:
                    count = glassdoor_reviews.find({'$and':[
                                                    {'company':{'$eq':company}},
                                                    {'quarter':{'$eq':quarter}},
                                                    {'review_date':{'$gte':datetime.datetime(year,1,1),
                                                                    '$lt':datetime.datetime(year+1,1,1)}
                                                    }]}).count()
                    #results.append([company, quarter, year, sum(sentiment)/len(sentiment), count])
                    results.append([company, quarter, year, sum(enums['recommend'])/len(enums['recommend']), sum(enums['outlook'])/len(enums['outlook']), sum(enums['ceo'])/len(enums['ceo'])])

    if len(results) > 100:

        with open(file,'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(results)

        results = []

except Exception as e:
    pass

finally:
    with open(file,'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(results)

