from pymongo import MongoClient
import datetime
from textblob import TextBlob
import csv


### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews

quarters = [1, 2, 3, 4]
years = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017]
companies = ['TEGNA']#glassdoor_reviews.distinct('company')

q_header = ['Year', 'Quarter', 'Company', 'Count', 'Sentiment_pol', 'Sentiment_sub', 'mgmt_sentiment','mgmt_sub', 'Rating', 'Recomendation', 'Outlook', 'Ceo']
q_file = 'features_by_quarter.csv'

y_header = ['Year', 'Company', 'Count', 'Sentiment_pol', 'Sentiment_sub', 'mgmt_sentiment', 'mgmt_sub', 'Rating', 'Recomendation', 'Outlook', 'Ceo']
y_file = 'features_by_year.csv'

# Returns polarity and subjectivity for a given text element
def perform_sentiment(text):
    tb = TextBlob(text)
    return (tb.sentiment.polarity, tb.sentiment.subjectivity)

def calculate_average(elements):
    if (len(elements) > 0):
        return sum(elements) / len(elements)
    else:
        return ''

def generate_features(query):

    pro_con_sentiment = []
    pro_con_subjectivity = []
    mgmt_polarity = []
    mgmt_subjectivity = []
    enums = {'rating': [], 'recommend': [], 'outlook': [], 'ceo': []}

    for doc in glassdoor_reviews.find(query):

        text = doc['pros'] + doc['cons']
        (pol,sub) = perform_sentiment(text)
        pro_con_sentiment.append(pol)
        pro_con_subjectivity.append(sub)

        if doc['advice_mgmt'] is not None:
            (pol, sub) = perform_sentiment(doc['advice_mgmt'])
            mgmt_polarity.append(pol)
            mgmt_subjectivity.append(sub)

        for enum in enums.keys():
            if doc[enum] is not None:
                enums[enum].append(doc[enum])

    if len(pro_con_sentiment) > 0:
        count = glassdoor_reviews.find(query).count()

        return [count,
                calculate_average(pro_con_sentiment),
                calculate_average(pro_con_subjectivity),
                calculate_average(mgmt_polarity),
                calculate_average(mgmt_subjectivity),
                calculate_average(enums['rating']),
                calculate_average(enums['recommend']),
                calculate_average(enums['outlook']),
                calculate_average(enums['ceo'])]


def process(enable_quarter):

    if enable_quarter:
        header = q_header
        file = q_file
    else:
        header = y_header
        file = y_file

    # Write out the header
    with open(file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

    # For each company
    for idx, company in enumerate(companies):

        print("Working on number: " + str(idx) + ", Name: " + company)

        # either quarters or years
        if enable_quarter:
            for year in years:
                for quarter in quarters:
                    result = [year, quarter, company]
                    query = {'$and': [
                                {'company': {'$eq': company}},
                                {'quarter': {'$eq': quarter}},
                                {'review_date': {'$gte': datetime.datetime(year, 1, 1),
                                                 '$lt': datetime.datetime(year + 1, 1, 1)}
                                 }]}
                    temp = generate_features(query)
                    if temp is not None:
                        result.extend(temp)
                        with open(file, 'a', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow(result)

        else:
            for year in years:
                result = [year, company]
                query = {'$and': [
                    {'company': {'$eq': company}},
                    {'review_date': {'$gte': datetime.datetime(year, 1, 1),
                                     '$lt': datetime.datetime(year + 1, 1, 1)}
                     }]}
                temp = generate_features(query)
                if temp is not None:
                    result.extend(temp)
                    with open(file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(result)

        print("Finishing: " + str(idx) + ", Name: " + company)

process(True)
process(False)

