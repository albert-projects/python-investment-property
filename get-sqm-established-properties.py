#!/usr/bin/env python3

# Module Imports
#import requests
import json
import re
from bs4 import BeautifulSoup
import time
import threading
import sqlite3
import js2py
import datetime
import csv
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
        self.pcode = pcode

    def run(self):
        # prev_month_year = "dummy"
        # prev_ptype = 0
        # prev_total = 0
        # data_list = []
        json_data = get_weburl(self.pcode)
        save_data_to_db(json_data)

def extract_mode_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    mode = query_params.get('mode')
    if mode:
        mode_number = mode[0]
        print(mode_number)
        return int(mode_number)
    else:
        return None

def get_weburl(postcode):
    json_list = []
    house_url = f"https://sqmresearch.com.au/graph.php?postcode={postcode}&mode=8&t=1"
    unit_url = f"https://sqmresearch.com.au/graph.php?postcode={postcode}&mode=10&t=1"
    comb_url = house_url + " " + unit_url
    url_list = comb_url.split()

    # Instantiate a web driver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
    # Set user agent string
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/93.0.4577.82 Safari/537.36"
    )
    options.add_argument(f"user-agent={user_agent}")
    print(f"running {postcode}")

    for u in url_list:
        # Set maximum number of retries
        max_retries = 2
        retry_count = 0
        #print(u)
        while retry_count < max_retries:
            # Randomly proxy pick an IP address and port
            # print(proxy)
            # driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
            # driver.delete_all_cookies()
            driver = webdriver.Chrome(options=options)
            retry_count += 1
            # Navigate to the webpage
            driver.get(u)

            try:
                dropdown = Select(driver.find_element("css selector", "select[name='mode']"))
                # Wait for the element to be present
                mode = extract_mode_number(u)
                #print(mode)
                if mode == 8:
                    dropdown.select_by_value("8")
                    target_div = "hichartcontainerDemo8"
                elif mode == 10:
                    dropdown.select_by_value("10")
                    target_div = "hichartcontainerDemo10"
                #print(target_div)
                element_present = EC.presence_of_element_located((By.ID, target_div))
                WebDriverWait(driver, 10).until(element_present)

                # Element found, break the loop
                break
            except:
                print(f"Attempt {retry_count}: Element not found, {u}")
                #print(driver.page_source)
                time.sleep(5)

        # After the loop, either the element is found or the maximum retries have been reached
        if retry_count == max_retries:
            print("Maximum retries reached, element not found.")
        else:
            #do something

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            # check if the postcode have data
            #house_div = soup.find('div', {'id': 'hichartcontainerDemo8'})
            pattern = r"series:\[\{name:[^}]*\},\{name:[^}]*\}\]"
            # find all script tags inside div with id plainInside
            if mode == 8:
                if soup.find('div', {'id': target_div}):
                    scripts = soup.select('#plainInside script')
                    result = re.findall(pattern, str(scripts))
                    json_data = result[0].replace("series:[{name: 'Postcode", '[{"name": "Postcode')
                    json_data = json_data.replace("',data:", '","data":')
                    json_data = json_data.replace(",{name: '", ',{"name": "')
                    tmp_list = [json_data, "House"]
                    json_list.append(tmp_list)
            elif mode == 10:
                if soup.find('div', {'id': target_div}):
                    scripts = soup.select('#plainInside script')
                    result = re.findall(pattern, str(scripts))
                    json_data = result[0].replace("series:[{name: 'Postcode", '[{"name": "Postcode')
                    json_data = json_data.replace("',data:", '","data":')
                    json_data = json_data.replace(",{name: '", ',{"name": "')
                    tmp_list = [json_data, "Unit"]
                    json_list.append(tmp_list)

    return json_list

def save_data_to_db(data_list):
    None_flag = 0
    # create connection and cursor
    conn = sqlite3.connect('database.db', timeout=10)
    cursor = conn.cursor()

    # # Create the table if it doesn't exist
    # cursor.execute('''
    #         CREATE TABLE IF NOT EXISTS sqm_established_properties (
    #             id INTEGER PRIMARY KEY AUTOINCREMENT,
    #             postcode TEXT NOT NULL,
    #             year TEXT,
    #             type TEXT,
    #             established_prop INTEGER,
    #             err_flag TEXT,
    #             create_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    #         )
    #     ''')

    #check if data = none
    for item in data_list:
        json_data, item_type = item
        #print(json_data)
        if json_data == 'None':
            # Skip processing 'None' records
            None_flag = 1

    if None_flag == 0:
        # Insert data into the table
        for item in data_list:
            json_data, item_type = item

            records = eval(json_data)  # Convert JSON string to Python object
            for record in records:
                if record['name'] != "":
                    postcode = record['name'].split()[-1]  # Extract the postcode from the name
                    pattern = r'^\d+$'  # Regular expression pattern for matching digits only
                    if re.match(pattern, postcode):
                        pcode = postcode
                for entry in record['data']:
                    year, established_prop = entry

                    # Check if the record already exists
                    cursor.execute('''
                            SELECT COUNT(*) FROM sqm_established_properties
                            WHERE postcode = ? AND year = ? AND type = ?
                        ''', (postcode, str(year), item_type))
                    count = cursor.fetchone()[0]

                    if count == 0 and postcode != "":
                        cursor.execute('''
                                INSERT INTO sqm_established_properties (postcode, year, type, established_prop, err_flag)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (postcode, str(year), item_type, established_prop, ''))

    # commit changes and close connection
    conn.commit()
    conn.close()
    if None_flag == 0:
        print(f"{pcode} wrote to database")
    elif None_flag == 1:
        print(f"None found in {pcode} json, skip record")

def postcode_done():
    # create an empty list to store the postcodes
    postcodes = []

    # open the text file and read each line
    with open('postcode_done.txt', 'r') as f:
        lines = f.readlines()

        # loop through each line and remove the newline character
        for line in lines:
            postcode = line.strip()

            # append the postcode to the list
            postcodes.append(postcode)
    #print(postcodes)
    return postcodes

def postcode_nodata():
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
    #print(postcodes)
    return postcodes

def split_list(lst, num):
    return [lst[i:i+num] for i in range(0, len(lst), num)]

def read_json():

    tmp_list = []
    # Read the JSON data from file
    with open('australian_postcodes.json') as f:
        data = json.load(f)

    # Loop through the data and print postcode values
    for item in data:
        #print(item['postcode'])
        tmp_list.append(item['postcode'])

    tmp2_list = list(set(tmp_list))
    tmp3_list = [num for num in tmp2_list if
                     (2000 <= int(num) <= 2999) or
                     (3000 <= int(num) <= 3999) or
                     (4000 <= int(num) <= 4999) or
                     (5000 <= int(num) <= 5799) or
                     (6000 <= int(num) <= 6799) or
                     (7000 <= int(num) <= 7799) or
                     (800 <= int(num) <= 899)]
    postcode_list = sorted(tmp3_list)

    # remove the postcode if it is done, the postcode read from a txt
    list_done = postcode_done()
    #print(list_done)
    postcode_list = [item for item in postcode_list if item not in list_done]
    list_nodata = postcode_nodata()
    postcode_list = [item for item in postcode_list if item not in list_nodata]
    #postcode_list = ['5109','3529','5012','3158','6000','4169']
    print(f"Number of postcode to do: {len(postcode_list)}")
    # split the list
    num = 10
    postcode_split = split_list(postcode_list, num)
    #print(postcode_split)

    # # split the list to 50 parts
    # num = 5
    # chunk_size = len(postcode_list) // num
    # remainder = len(postcode_list) % num
    # postcode_splits = []
    # start = 0
    #
    # for i in range(num):
    #     if i < remainder:
    #         end = start + chunk_size + 1
    #     else:
    #         end = start + chunk_size
    #     postcode_splits.append(postcode_list[start:end])
    #     start = end

    #print(len(postcode_splits))
    #print(postcode_splits)
    #print(postcode_splits[1])
    #print(postcode_splits[num-1])
    #print(postcode_list)

    # multi threading
    #postcode_list = [['2386']]
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

read_json()