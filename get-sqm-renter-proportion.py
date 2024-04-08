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


def save_data_to_db(data_list):
    None_flag = 0
    # create connection and cursor
    conn = sqlite3.connect('database.db', timeout=10)
    cursor = conn.cursor()

    # check if data = none
    for item in data_list:
        json_data, OccupantType, postcode = item
        # print(json_data)
        if json_data == 'None':
            # Skip processing 'None' records
            None_flag = 1

    if None_flag == 0:
        joined_result_list = []
        joined_result_list1 = []
        joined_result_list2 = []
        joined_result_list3 = []
        tmp_list_oo = []
        tmp_list_mh = []
        tmp_list_r = []
        tmp_list_o = []
        tmp_list_ns = []
        # Insert data into the table
        for item in data_list:
            json_data, OccupantType, postcode = item
            #print(json_data, OccupantType, postcode)
            records = eval(json_data)  # Convert JSON string to Python object
            for record in records:
                if record['name'] == "Owner outright":
                    for entry in record['data']:
                        year, owner_outright = entry
                        #print(f"Owner outright: {postcode}, {year}, {owner_outright}")
                        tmp_str = (f'{postcode},{year},{owner_outright}')
                        tmp_list_oo.append(tmp_str)
                if record['name'] == "Mortgage Holders":
                    for entry in record['data']:
                        year, mortgage_holders = entry
                        #print(f"Mortgage Holders: {postcode}, {year}, {mortgage_holders}")
                        tmp_str = (f'{postcode},{year},{mortgage_holders}')
                        tmp_list_mh.append(tmp_str)
                if record['name'] == "Rented":
                    for entry in record['data']:
                        year, rented = entry
                        tmp_str = (f'{postcode},{year},{rented}')
                        tmp_list_r.append(tmp_str)
                if record['name'] == "Others":
                    for entry in record['data']:
                        year, others = entry
                        tmp_str = (f'{postcode},{year},{others}')
                        tmp_list_o.append(tmp_str)
                if record['name'] == "Not stated":
                    for entry in record['data']:
                        year, not_stated = entry
                        tmp_str = (f'{postcode},{year},{not_stated}')
                        tmp_list_ns.append(tmp_str)

        # print(tmp_list_oo)
        # print(tmp_list_mh)
        # print(tmp_list_r)
        # print(tmp_list_o)
        # print(tmp_list_ns)

        # Create a dictionary to store the joined data
        joined_data = {}

        # Iterate over each list and join the data
        for data_list in [tmp_list_oo, tmp_list_mh, tmp_list_r, tmp_list_o, tmp_list_ns]:
            for item in data_list:
                postcode, year, value = item.split(',')

                # Create a unique key based on postcode and year
                key = f"{postcode}_{year}"

                # Check if the key already exists in the dictionary
                if key in joined_data:
                    joined_data[key].append(value)
                else:
                    joined_data[key] = [value]

        # Print the joined data
        for key, values in joined_data.items():
            postcode, year = key.split('_')
            #print(f"Postcode: {postcode}, Year: {year}, Values: {', '.join(values)}")
            #print(values)
            # Define the SQL query template
            sql = '''INSERT INTO sqm_occupant_type (postcode, year, owner_outright, mortgage_holders, rented, others, not_stated)
                     VALUES (?, ?, ?, ?, ?, ?, ?)'''
            # Prepare the values for the SQL query
            sql_values = (postcode, year, values[0], values[1], values[2], values[3], values[4])
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
        print(f"None found in {postcode} json, skip record")

def get_weburl(postcode):
    json_list = []
    url = f"https://sqmresearch.com.au/graph.php?postcode={postcode}&mode=2&t=1"

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

    # Set maximum number of retries
    max_retries = 2
    retry_count = 0
    # print(u)
    while retry_count < max_retries:
        # Randomly proxy pick an IP address and port
        # print(proxy)
        # driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        # driver.delete_all_cookies()
        driver = webdriver.Chrome(options=options)
        retry_count += 1
        # Navigate to the webpage
        driver.get(url)

        try:
            dropdown = Select(driver.find_element("css selector", "select[name='mode']"))
            # Wait for the element to be present
            mode = extract_mode_number(url)
            # print(mode)
            if mode == 2:
                dropdown.select_by_value("2")
                target_div = "hichartcontainerDemo2"
            # print(target_div)
            element_present = EC.presence_of_element_located((By.ID, target_div))
            WebDriverWait(driver, 10).until(element_present)

            # Element found, break the loop
            break
        except:
            print(f"Attempt {retry_count}: Element not found, {url}")
            # print(driver.page_source)
            time.sleep(5)

    # After the loop, either the element is found or the maximum retries have been reached
    if retry_count == max_retries:
        print("Maximum retries reached, element not found.")
    else:
        # do something
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # check if the postcode have data
        # house_div = soup.find('div', {'id': 'hichartcontainerDemo8'})
        pattern = r"series:\[\{name: 'Owner outright',data:[^}]*\},\{name: 'Mortgage Holders',data:[^}]*\},{name: 'Rented',data:[^}]*\},{name: 'Others',data:[^}]*\},{name: 'Not stated',data:[^}]*\}\]"
        # find all script tags inside div with id plainInside
        if mode == 2:
            if soup.find('div', {'id': target_div}):
                scripts = soup.select('#plainInside script')
                result = re.findall(pattern, str(scripts))
                #print(result[0])
                json_data = result[0].replace("series:[{name: 'Owner outright", '[{"name": "Owner outright')
                json_data = json_data.replace("name: 'Mortgage Holders", '"name": "Mortgage Holders')
                json_data = json_data.replace("name: 'Rented", '"name": "Rented')
                json_data = json_data.replace("name: 'Others", '"name": "Others')
                json_data = json_data.replace("name: 'Not stated", '"name": "Not stated')
                json_data = json_data.replace("',data:", '","data":')
                tmp_list = [json_data, "OccupantType", postcode]
                json_list.append(tmp_list)
    #print(json_list)
    return json_list

def extract_mode_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    mode = query_params.get('mode')
    if mode:
        mode_number = mode[0]
        #print(mode_number)
        return int(mode_number)
    else:
        return None

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
    #postcode_list = ['4000','2400']
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