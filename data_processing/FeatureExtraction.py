from pymongo import MongoClient
import datetime
from textblob import TextBlob
import csv


### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews

quarters = [1,2,3,4]
years = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]
file = 'features'
header = ['Company', 'Quarter', 'Year','Sentiment_pol', 'Sentiment_sub' 'Rating', 'Recomendation', 'Outlook', 'Ceo']

companies = glassdoor_reviews.distinct('company')

# write the header
with open(file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)

def ExtractFeatures(evaluate_quarters):

    if evaluate_quarters:


    try:
        for idx, company in enumerate(companies):
            print("Working on number: " + str(idx) + ", Name: " + company)
            for year in years:

                if evaluate_quarters:
                    for quarter in quarters:

                        # Evaluate the sentiment for each review during the quarter and average

                        for doc in glassdoor_reviews.find()


                        for doc in glassdoor_reviews.aggregate([
                                        {'$match':{'$and':[ {'company':{'$eq':company}},
                                                            {'quarter':{'$eq':quarter}},
                                                            {'review_date':{'$gte':datetime.datetime(year,1,1),
                                                                            '$lt':datetime.datetime(year+1,1,1)}
                                                            }]}},
                                        {'$group': {'_id': None,'rating':{'$avg':'$rating'},'recommend': {'$avg': '$recommend'},'outlook':{'$avg':'$outlook'},'ceo':{'$avg':'$ceo'}}}

                                    ] ):
                         result = doc['pros'] + doc['cons']
                         tb = TextBlob(result)
                         sentiment.append(tb.sentiment.polarity)
                else:

                    # Evaluate the sentiment for each review during the year, and average

                    for doc in glassdoor_reviews.aggregate([
                        {'$match': {'$and': [{'company': {'$eq': company}},
                                             {'review_date': {'$gte': datetime.datetime(year, 1, 1),
                                                              '$lt': datetime.datetime(year + 1, 1, 1)}
                                              }]}},
                        {'$group': {'_id': None, 'rating': {'$avg': '$rating'}, 'recommend': {'$avg': '$recommend'},
                                    'outlook': {'$avg': '$outlook'}, 'ceo': {'$avg': '$ceo'}}}

                    ]):


                        results.append([company, quarter, year, sum(sentiment)/len(sentiment), count])
                        results.append([company, quarter, year, doc['rating'],doc['recommend'],doc['outlook'],doc['ceo']])


    except Exception as e:
        pass

    finally:
        with open(file,'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(results)



