import csv
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import time
from random import randint
import traceback

### User Agents for requests
ua = UserAgent()
header = {'User-Agent': str(ua.chrome)}

'''
Queries the base URL
'''
def query_website(company_name):

    url = 'https://www.glassdoor.com/Reviews/' + company_name.lower() + '-reviews-SRCH_KE0,' + str(len(company_name))+ '.htm'
    print("Querying: " + url)
    result = requests.get(url, headers=header)

    if result.status_code == 200:
        return result.content
    else:
        raise RuntimeError('Request returned ' + str(result.status_code))


'''
Converts HTML to list of python dictionaries
'''
def parse_html(content) -> []:

    max = -1
    url = ''
    reviews = ''

    soup = BeautifulSoup(content, 'lxml')

    # Glassdoor auto redirects to the exact page
    if ('working' in soup.title.string.lower() or 'career' in soup.title.string.lower()):
        data = soup.find('a', class_='eiCell cell reviews ')
        url = data['href']
        reviews = data.find('span', class_='num h2').getText()

    else:  # search page
        companyList = soup.findAll('div', class_='eiHdrModule module snug ')

        # For each on the first page, find the one with the most reviews
        for company in companyList:
            temp_url = safe_dictionary_lookup(company, 'a', 'eiCell cell reviews', 'href')
            temp_reviews = safe_text_find(company, 'span', 'num h2')

            # if it has a k, convert it to an int
            if 'k' in temp_reviews:
                temp_reviews = str(float(temp_reviews[:-1]) * 1000)

            if (int(temp_reviews) > max):
                max = int(temp_reviews)
                url = temp_url
                reviews = temp_reviews

    return url, reviews

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

with open('remaining.csv') as csvfile:
    companyreader = csv.reader(csvfile, delimiter=',')
    for row in companyreader:
        companies.append(row[0])

for company in companies:

    results = []

    try:

        html_content = query_website(company)

        url, reviews = parse_html(html_content)

        result = [company, url, reviews]
        results.append(result)

        wait = randint(120, 240)
        print("waiting " + str(wait) + " seconds")
        time.sleep(wait)

    except Exception as e:
        print("Issue with " + str(company))
        print(str(e))
        print("============================")
        print(traceback.format_exc())

    finally:
        with open('results.csv', 'a') as f:
            writer = csv.writer(f)
            for item in results:
                writer.writerow([item[0], item[1], item[2]])


