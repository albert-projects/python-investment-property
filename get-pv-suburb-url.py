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
import datetime
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
#from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs

class ScrapeThread(threading.Thread):
    def __init__(self, pcode):
        #threading.Thread.__init__(self)
        super(ScrapeThread, self).__init__()
        self.pcode = pcode[1]
        self.url = pcode[0]

    def run(self):
        # prev_month_year = "dummy"
        # prev_ptype = 0
        # prev_total = 0
        # data_list = []
        #json_data = get_weburl(self.pcode)
        #save_data_to_db(json_data)
        #print(self.pcode, self.url)
        url_data = get_weburl(self.pcode, self.url)
        save_data_to_db(url_data)


def save_data_to_db(data_list):
    None_flag = 0
    #print(data_list)
    # create connection and cursor
    conn = sqlite3.connect('database.db', timeout=10)
    cursor = conn.cursor()

    # check if data = none
    for item in data_list:
        postcode, suburb, url, href = item
        # print(json_data)
        if postcode == 'nodata':
            # Skip processing 'None' records
            pcode = href
            None_flag = 1

    if None_flag == 0:
        for item in data_list:
            postcode, suburb, url, href = item
            sql = '''INSERT INTO pv_suburb_url (postcode, suburb, suburb_url, original_url )
                                 VALUES (?, ?, ?, ?)'''
            sql_values = (postcode, suburb, url, href)
            #print(sql_values)
            # Execute the SQL query
            cursor.execute(sql, sql_values)

    # commit changes and close connection
    conn.commit()
    # Close the cursor and database connection
    cursor.close()
    conn.close()

    if None_flag == 0:
        print(f"{postcode} wrote to database")
    elif None_flag == 1:
        print(f"None found in {pcode}, skip record")


def get_weburl(pcode, url):
    #print(postcode, url)
    print(f'Running {pcode}')
    data_list = []
    # Send a GET request to the URL
    response = requests.get(url)
    # Get the HTML content from the response
    html = response.content
    # Create BeautifulSoup object
    soup = BeautifulSoup(html, 'html.parser')

    # Find all div elements with class="suburbList"
    suburb_list_div = soup.find('div', class_='suburbList')
    div = str(suburb_list_div).replace('\n', '').strip()
    #print(str(suburb_list_div).replace('\n','').strip())
    #check the element if loaded
    if (suburb_list_div is not None) and div != '<div class="suburbList"></div>':
        # Find all a elements within the suburbList div
        a_elements = suburb_list_div.find_all('a')

        # Extract the href and text for each a element
        data = [(urllib.parse.quote(a['href']), a.get_text(strip=True)) for a in a_elements]
        #data_list = eval(data)
        #print(type(data), data)
        #converted_data = []
        for item in data:
            href = str(item[0])
            suburb = str(item[1])
            # Extract the middle part from the href
            middle_part = href.split('/')[2]
            # Extract the postcode and state from the middle part
            postcode, state, sub_url = middle_part.split('-')[-1], middle_part.split('-')[-2], middle_part.split('-')[-3]
            converted_url = f"https://www.propertyvalue.com.au/suburb/{sub_url}-{postcode}-{state}"
            tmp_data = f'{postcode},{suburb},{converted_url},{href}'.split(',')
            #print(middle_part, postcode, state, sub_url)
            data_list.append(tmp_data)
    else:
        tmp_data = f'nodata,nodata,nodata,{pcode}'.split(',')
        data_list.append(tmp_data)
    #print(data_list)
    return data_list

def postcode_done(data_list):
    # create an empty list to store the postcodes
    postcodes = []

    # open the text file and read each line
    with open('pv_postcode_done.txt', 'r') as f:
        lines = f.readlines()

        # loop through each line and remove the newline character
        for line in lines:
            postcode = line.strip()

            # append the postcode to the list
            postcodes.append(postcode)

    # filter the data list based on postcodes
    filtered_data = [item for item in data_list if item[1] not in postcodes]

    return filtered_data

def postcode_nodata(data_list):
    # create an empty list to store the postcodes
    postcodes = []

    # open the text file and read each line
    with open('nodata.txt', 'r') as f:
        lines = f.readlines()

        # loop through each line and remove the newline character
        for line in lines:
            postcode = line.strip()

            # append the postcode to the list
            postcodes.append(postcode)

    # filter the data list based on postcodes
    filtered_data = [item for item in data_list if item[1] not in postcodes]

    return filtered_data

def split_list(lst, num):
    return [lst[i:i+num] for i in range(0, len(lst), num)]

def read_csv():

    tmp_list = []
    file_path = 'propertyvalue.csv'

    # Loop through the data and print postcode values
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            fullurl = row[0]
            postcode = row[1]
            tmp_list.append((fullurl, postcode))

    tmp2_list = list(set(tmp_list))
    #print(type(tmp2_list[0]), type(tmp2_list[1]))
    tmp3_list = []
    for item in tmp2_list:
        try:
            postcode = int(item[1])
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

    postcode_list = sorted(tmp3_list, key=lambda x: x[1])
    # remove the postcode if it is done, the postcode read from a txt
    #list_done = postcode_done()
    #print(list_done)
    #postcode_list = [item for item in postcode_list if item[1] not in list_done]
    list_nodata = postcode_nodata(postcode_list)
    postcode_list = postcode_done(list_nodata)
    #postcode_list = [item for item in postcode_list if item[1] not in list_nodata]
    #postcode_list = ['4000','2400']
    #postcode_list = [('https://www.propertyvalue.com.au/postcode/nt/0828', '0828')]
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

read_csv()