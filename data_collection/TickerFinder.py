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

    url = 'https://www.bloomberg.com/markets/symbolsearch?query=' + company_name.replace(' ', '+') + '&commit=Find+Symbols'
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
    data = soup.find('td', class_='symbol')
    if data is not None:
        ticker = data.find('a').getText()
    else:
        ticker = "Issue"
    return ticker


#########################################################
#
# Main
#
#########################################################

companies = []

with open(r'..\csvs\tickersNeeded.csv') as csvfile:
    companyreader = csv.reader(csvfile, delimiter=',')
    for row in companyreader:
        companies.append(row[0])

for company in companies:

    results = []

    try:

        html_content = query_website(company)

        ticker = parse_html(html_content)

        ## Try again dropping the last word
        if (ticker == 'Issue'):
            content = query_website(company.rsplit(' ',1)[0])
            ticker = parse_html(content)

        result = [company, ticker]
        results.append(result)

        wait = randint(10, 60)
        print("waiting " + str(wait) + " seconds")
        time.sleep(wait)

    except Exception as e:
        print("Issue with " + str(company))
        print(str(e))
        print("============================")
        print(traceback.format_exc())

    finally:
        with open(r'..\csvs\tickerList.csv', 'a') as f:
            writer = csv.writer(f)
            for item in results:
                writer.writerow([item[0], item[1]])


