import csv
from bs4 import BeautifulSoup
from pymongo import MongoClient
import requests
from fake_useragent import UserAgent
import time
from random import randint
import traceback
import math

### DB connections
client = MongoClient()
glassdoor_db = client.glassdoor
glassdoor_reviews = glassdoor_db.reviews

### User Agents for requests
ua = UserAgent()
header = {'User-Agent': str(ua.chrome)}

class Company:

    def __init__(self, company_name: str, url: str, total_reviews: int) -> None:
        self.cn = company_name
        self.url = url.split(".")[0]
        self.reviews = total_reviews


'''
Queries the base URL
'''
def query_website(cmpy : Company, page_num: int):

    url = 'https://www.glassdoor.com' + cmpy.url + '_P' + str(page_num) + '.htm'
    print("Querying: " + url)
    result = requests.get(url, headers=header)

    if result.status_code == 200:
        return result.content
    else:
        raise RuntimeError('Request returned' + str(result.status_code))


'''
Converts HTML to list of python dictionaries
'''
def parse_html(content, cmpy: Company) -> []:
    soup = BeautifulSoup(content)
    result = []

    reviews = soup.find_all('li', class_='empReview')

    for review in reviews:
        first_section = {'company': cmpy.cn,
                         'review_date': safe_dictionary_lookup(review, 'time', 'date', 'datetime'),
                         'review_title': safe_text_find(review, 'span', 'summary'),
                         'job_title': safe_text_find(review, 'span', 'authorJobTitle'),
                         'location': safe_text_find(review, 'span', 'authorLocation'),
                         'rating': safe_dictionary_lookup(review, 'span', 'value-title', 'title'),
                         'duration': safe_text_find(review, 'p', 'mainText'),
                         'pros': safe_text_find(review, 'p', 'pros'),
                         'cons': safe_text_find(review, 'p', 'cons'),
                         'advice_mgmt': safe_text_find(review, 'p', 'adviceMgmt'),
                         'helpful': safe_text_find(review, 'span', 'helpfulCount'),
                         'url': safe_dictionary_lookup(review, 'a','reviewLink', 'href')
                       }
        second_section = parse_categories(review)

        result.append({**first_section, **second_section})

    return result


'''
Flex-grid contains between 0-3 elements
This method safely determines which is not null and returns the span value
'''
def parse_categories(review):

    result = {'recommend': None, 'outlook': None, 'ceo': None}

    future_expect = review.find(class_='flex-grid')

    if future_expect:
        for section in future_expect.findAll('span', class_='middle'):
            if 'rec' in section.getText().lower():
                result['recommend'] = section.getText()
            elif 'outl' in section.getText().lower():
                result['outlook'] = section.getText()
            elif 'ceo' in section.getText().lower():
                result['ceo'] = section.getText()

    return result

'''
Returns none if the node does not exist
'''
def safe_text_find(review, attrib, clazz):
    return None if not review.find(attrib, class_=clazz) else review.find(attrib, class_=clazz).getText()

'''
Returns None if the key-value pair does not exist
'''
def safe_dictionary_lookup(review, attrib, clazz, key):
    review_dict = review.find(attrib, class_=clazz)
    if review_dict:
        if key in review_dict.attrs:
            return review_dict[key]
    return None


#########################################################
#
# Main
#
#########################################################

companies = []

with open('../csvs/results_complete.csv') as csvfile:
    companyreader = csv.reader(csvfile, delimiter=',')
    for row in companyreader:
        companies.append(Company(row[0], row[1], int(row[2])))

for company in companies:

    # Only get 1000 reviews
    max_requests = 100 if company.reviews > 1000 else math.ceil(company.reviews // 10)

    mongo_reviews = []

    try:
        for idx in range(1, max_requests+1):
            html_content = query_website(company, idx)

            mongo_reviews.extend(parse_html(html_content,company))

            # Perodically write to the DB
            if (len(mongo_reviews) > 149):
                glassdoor_reviews.insert_many(mongo_reviews)
                mongo_reviews = []
            wait = randint(5, 45)
            print("waiting " + str(wait) + " seconds")
            time.sleep(wait)

        print("Completed: " + str(company))

    except Exception as e:
        print("Issue with " + str(company))
        print(str(e))
        print("============================")
        print(traceback.format_exc())

    finally:
        if len(mongo_reviews) > 0:
            glassdoor_reviews.insert_many(mongo_reviews)




















