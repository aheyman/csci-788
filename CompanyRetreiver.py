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
    soup = BeautifulSoup(content, 'lxml')

    company = soup.find('div', class_='eiHdrModule module snug ')

    url = company.find('a', class_='eiCell cell reviews')['href']
    reviews = company.find('span', class_='num h2').getText()

    return url, reviews


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

        if len(result) > 0:
            break

        wait = randint(120, 300)
        print("waiting " + str(wait) + " seconds")
        time.sleep(wait)

    except Exception as e:
        print("Issue with " + str(company))
        print(str(e))
        print("============================")
        print(traceback.format_exc())

    finally:
        with open('results.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['company', 'url', 'reviews'])
            for item in results:
                writer.writerow([item[0], item[1], item[2]])


