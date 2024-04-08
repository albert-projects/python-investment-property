#!/usr/bin/env python3

# Module Imports
import requests
import json
import re
from bs4 import BeautifulSoup
import time
import threading
import sqlite3
import js2py
from datetime import datetime
import csv
import urllib.parse
import random

#from requests_html import HTMLSession
from selenium import webdriver
#from selenium.webdriver.common.action_chains import ActionChains
#from selenium.webdriver.chrome.options import Options
#from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, ElementNotVisibleException
#from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs


class ScrapeThread(threading.Thread):
    def __init__(self, pcode):
        #threading.Thread.__init__(self)
        super(ScrapeThread, self).__init__()
        self.pcode = pcode[0]
        self.suburb = pcode[1]
        self.url = pcode[2]

    def run(self):
        # prev_month_year = "dummy"
        # prev_ptype = 0
        # prev_total = 0
        # data_list = []
        #json_data = get_weburl(self.pcode)
        #save_data_to_db(json_data)
        #print(self.pcode, self.url, self.suburb)
        url_data = get_weburl(self.pcode, self.url, self.suburb)
        #print(url_data)
        save_data_to_db(url_data)


def save_data_to_db(data_list):
    None_flag = 0
    #print(data_list)
    # create connection and cursor
    conn = sqlite3.connect('database.db', timeout=10)
    cursor = conn.cursor()
    # check if data = none
    for item in data_list:
        postcode, suburb, median_value, properties_sold, median_rent, median_gross_yield, average_days_on_market, average_vendor_discount, median_price_change_1yr, data_time, suburb_url = item
        # print(json_data)
        if postcode == 'nodata':
            # Skip processing 'None' records
            pcode = median_value
            sub = properties_sold
            None_flag = 1

    if None_flag == 0:
        for item in data_list:
            postcode, suburb, median_value, properties_sold, median_rent, median_gross_yield, average_days_on_market, average_vendor_discount, median_price_change_1yr, data_time, suburb_url = item
            sql = '''INSERT INTO pv_market_trends (postcode, suburb, median_value, properties_sold, median_rent, median_gross_yield, average_days_on_market, average_vendor_discount, median_price_change_1yr, data_time, suburb_url )
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            sql_values = (postcode, suburb, median_value, properties_sold, median_rent, median_gross_yield, average_days_on_market, average_vendor_discount, median_price_change_1yr, data_time, suburb_url)
            cursor.execute(sql, sql_values)

    # commit changes and close connection
    conn.commit()
    # Close the cursor and database connection
    cursor.close()
    conn.close()

    if None_flag == 0:
        print(f"{postcode} {suburb} wrote to database")
        save_url(suburb_url)
    elif None_flag == 1:
        print(f"None found in {pcode} {sub}, skip record")

def save_url(url):
    with open("pv_url_done.txt", "a") as file:
        file.write(url + "\n")
    #print(f"Postcode {postcode} saved to postcode_done.txt")

def get_weburl(pcode, url, suburb):
    #print(postcode, url)
    print(f'Running {pcode}, {suburb}')
    data_list = []
    house_url = f"{url}#House"
    # # Instantiate a web driver
    # options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
    # # Set user agent string
    # user_agent = (
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    #     "Chrome/93.0.4577.82 Safari/537.36"
    # )
    # options.add_argument(f"user-agent={user_agent}")

    # Set maximum number of retries
    max_retries = 2
    retry_count = 0
    # print(u)
    while retry_count < max_retries:
        # Randomly proxy pick an IP address and port
        # print(proxy)
        # driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        # driver.delete_all_cookies()
        #driver = webdriver.Chrome(options=options)
        response = requests.get(house_url)
        # Get the HTML content from the response
        html = response.content
        # Create BeautifulSoup object
        soup = BeautifulSoup(html, 'html.parser')
        retry_count += 1

        # Navigate to the webpage
        #driver.get(house_url)
        try:
            # Wait for the element to be present
            # print(mode)
            # print(target_div)
            #xpath_expression = "//a[contains(@class, 'marketTrend') and contains(@onclick, \"FreemiumMarketTrends.getMarketTrendsHighChart(11,this,'Median Value','Houses','FreemiumSuburb')\")]"
            #element_present = EC.presence_of_element_located((By.XPATH, xpath_expression))
            #WebDriverWait(driver, 10).until(element_present)
            # Click on the element
            #element = driver.find_element(By.XPATH, xpath_expression)
            soup = BeautifulSoup(html, 'html.parser')
            div_id = "market-trends-metric-box-values"
            div_element = soup.find('div', id=div_id)
            break
        except:
            print(f"Attempt {retry_count}: Median Value not found, {house_url}")
            # print(driver.page_source)
            time.sleep(5)

    #print(div_element)
    if div_element is not None:
        # Find all the list items within the div element
        list_items = div_element.find_all('li')
        # Create a dictionary to store the extracted data
        data = []
        # Extract the data from each list item
        for item in list_items:
            # Get the metric name
            metric_name = item.find('span', class_='text').get_text(strip=True)

            # Get the metric value based on the id attribute using a wildcard pattern
            metric_value_element = item.find('span', id=lambda value: value and value.startswith('metric-box-'))
            metric_value = metric_value_element.get_text(strip=True) if metric_value_element else None

            # Store the data in the dictionary
            tmp_str = f'{metric_name},{metric_value}'.split(',')
            data.append(tmp_str)

        # Print the extracted data
        # for metric, value in data.items():
        #     print(f'{metric} = {value}')
        month_year = get_current_month_year()
        tmp_list = f'{pcode},{suburb},{data[0][1]},{data[1][1]},{data[2][1]},{data[3][1]},{data[4][1]},{data[5][1]},{data[6][1]},{month_year},{url}'.split(',')
        data_list.append(tmp_list)
    else:
        save_url_to_file(url, pcode, suburb)
        tmp_data = f'nodata,nodata,{pcode},{suburb},nodata,nodata,nodata,nodata,nodata,nodata,nodata'.split(',')
        data_list.append(tmp_data)
    return data_list

def save_url_to_file(url, pcode, suburb):
    with open("pv_nodata.txt", "a") as file:
        file.write(url + "\n")
    print(f"{pcode}, {suburb} saved to pv_nodata.txt")

def get_current_month_year():
    # Get the current date
    current_date = datetime.now()

    # Format the date as "MMM YYYY" (e.g., "Apr 2023")
    formatted_date = current_date.strftime("%b %Y")

    return formatted_date

def postcode_done(data_list):
    # create an empty list to store the postcodes
    postcodes = []

    # open the text file and read each line
    with open('pv_url_done.txt', 'r') as f:
        lines = f.readlines()

        # loop through each line and remove the newline character
        for line in lines:
            postcode = line.strip()

            # append the postcode to the list
            postcodes.append(postcode)

    # filter the data list based on postcodes
    filtered_data = [item for item in data_list if item[2] not in postcodes]

    return filtered_data

def postcode_nodata(data_list):
    # create an empty list to store the postcodes
    postcodes = []

    # open the text file and read each line
    with open('pv_nodata.txt', 'r') as f:
        lines = f.readlines()

        # loop through each line and remove the newline character
        for line in lines:
            postcode = line.strip()

            # append the postcode to the list
            postcodes.append(postcode)

    # filter the data list based on postcodes
    filtered_data = [item for item in data_list if item[2] not in postcodes]

    return filtered_data

def split_list(lst, num):
    return [lst[i:i+num] for i in range(0, len(lst), num)]

def read_url():

    tmp_list = []
    # Connect to the database
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Execute the query to select data from the table
    cursor.execute("SELECT postcode, suburb, suburb_url FROM pv_suburb_url")
    # Fetch all rows from the result set
    rows = cursor.fetchall()
    # Iterate over the rows and append the data to the list
    for row in rows:
        tmp_list.append(row)

    tmp2_list = list(set(tmp_list))
    #print(type(tmp2_list[0]), type(tmp2_list[1]))
    tmp3_list = []
    for item in tmp2_list:
        try:
            postcode = int(item[0])
            if (
                    2000 <= postcode <= 2999 or
                    3000 <= postcode <= 3999 or
                    4000 <= postcode <= 4999 or
                    5000 <= postcode <= 5799 or
                    6000 <= postcode <= 6799 or
                    7000 <= postcode <= 7799 or
                    800 <= postcode <= 899
            ):
                tmp3_list.append(item)
        except ValueError:
            pass

    postcode_list = sorted(tmp3_list, key=lambda x: x[0])
    # remove the postcode if it is done, the postcode read from a txt
    #list_done = postcode_done()
    #print(postcode_list)
    #postcode_list = [item for item in postcode_list if item[1] not in list_done]
    list_nodata = postcode_nodata(postcode_list)
    postcode_list = postcode_done(list_nodata)
    #postcode_list = [item for item in postcode_list if item[1] not in list_nodata]
    #postcode_list = ['4000','2400']
    #postcode_list = [('https://www.propertyvalue.com.au/postcode/nt/0828', '0828')]
    #postcode_list = [('7470', 'Rosebery', 'https://www.propertyvalue.com.au/suburb/rosebery-7470-tas')]
    print(f"Number of postcode to do: {len(postcode_list)}")
    #print(postcode_list)
    # split the list
    num = 10
    postcode_split = split_list(postcode_list, num)
    #print(postcode_split)

    # multi threading
    # assign the postcode to grip data
    thread_ind = 0
    for i in postcode_split:
        print(f"Thread {thread_ind} start")
        threads = []
        for postcode in i:
            #print(f"Running {postcode}")
            t = ScrapeThread(postcode)
            t.start()
            threads.append(t)
            #print(f"Done {postcode}")

        for t in threads:
            t.join()
        print(f"Thread {thread_ind} done")
        thread_ind += 1

read_url()