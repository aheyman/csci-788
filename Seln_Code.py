from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
import json
import requests
from fake_useragent import UserAgent

## Sign into Glassdoor
drivers = [webdriver.Chrome(), webdriver.Firefox(), webdriver.Ie(), webdriver.Edge() ]

drivers[0].get('https://www.glassdoor.com/profile/login_input.htm?userOriginHook=HEADER_SIGNIN_LINK')
drivers[0].find_element_by_id("signInUsername").sendKeys("bugmenot@mailinator.com")
drivers[0].find_element_by_id("signInPassword").sendKeys("bugmenot")
drivers[0].find_element_by_id("signInBtn").click()

#for each comany[]

## Pull the company
options = webdriver.ChromeOptions()
options.add_argument(argument="--incognito")
driver = webdriver.Chrome(r'C:/Users/Andrew/Downloads/chromedriver_win32/chromedriver.exe', chrome_options=options)

driver.get('https://www.glassdoor.com/Reviews/JetBlue-Reviews-E11385_P4.htm')
html = driver.page_source
