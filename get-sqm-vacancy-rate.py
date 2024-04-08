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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
#from webdriver_manager.chrome import ChromeDriverManager

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

        # Set maximum number of retries
        max_retries = 2
        retry_count = 0

        # Instantiate a web driver
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
        # Set user agent string
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/93.0.4577.82 Safari/537.36"
        )
        options.add_argument(f"user-agent={user_agent}")
        print(f"running {self.pcode}")

        while retry_count < max_retries:
            #options.add_argument('--proxy-server={}'.format(proxy))
            #print(proxy)
            #driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
            #driver.delete_all_cookies()
            driver = webdriver.Chrome(options=options)
            retry_count += 1
            success = 1
            # Navigate to the webpage
            url = f'https://sqmresearch.com.au/graph_vacancy.php?postcode={self.pcode}&t=1'
            driver.get(url)

            try:
                # Wait for the element to be present
                element_present = EC.presence_of_element_located((By.ID, "hichartcontainerVR"))
                WebDriverWait(driver, 5).until(element_present)
                #print(element_present)
                #success = 1
                # Element found, break the loop
                break
            except:
                print(f"Attempt {retry_count}: Element not found, {url}")
                #print(driver.page_source)
                success = 0
                time.sleep(30)

        # After the loop, either the element is found or the maximum retries have been reached
        if retry_count == max_retries and success == 0:
            print("Maximum retries reached, element not found.")
        else:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            # check if the postcode have data
            div = soup.find('div', {'id': 'hichartcontainerVR'})
            # print(driver.page_source)

            if div and div.text.strip():
                title = soup.find('div', {'id': 'plainInside'}).find('h1').text.strip()
                #print(title)
                pcode = soup.find('div', {'id': 'plainInside'}).find('h2').text.strip()
                # Separate the string using split() method and get the last element <h2>Postcode 6210</h2>
                postcode = pcode.split()[-1]
                # Trim the string using slice notation and get the last 4 digits
                postcode = postcode[-4:]
                if not postcode.isnumeric():
                    # print("Invalid postcode")
                    postcode = 0000
                scripts = soup.select('#plainInside script')
                for script in scripts:
                     if script.string and script.string.strip().startswith('eval'):
                        # extract the data string from the eval statement
                        data = script.string.strip()
                        data = data.replace("eval", "")
                        decoded_data = js2py.eval_js(data)
                        #print(decoded_data)

                        # Regular expression to match the data
                        pattern_vacancies = r"{name:'Vacancies',.*?data:(\[\[.*?\]\])}"
                        pattern_vacancy_rate = r"{name:'Vacancy Rate',.*?data:(\[\[.*?\]\])}"

                        match_vacancies = re.search(pattern_vacancies, decoded_data)
                        match_vacancy_rate = re.search(pattern_vacancy_rate, decoded_data)
                        if match_vacancies and match_vacancy_rate:
                            data_vacancies = match_vacancies.group(1)
                            data_vacancy_rate = match_vacancy_rate.group(1)
                            json_data = '[{"name": "Vacancies", "data":' + data_vacancies + '},' + '{"name":"VacancyRate","data":' + data_vacancy_rate + '}]'
                            #print(f"{postcode}, {json_data}")
                            load_decoded_json(json_data, postcode)
            else:
                print(f"No_data, {self.pcode}")

def load_decoded_json(json_data, postcode):

    data = json.loads(json_data)
    csv_i = 0
    csv_j = 0
    data_list = []
    tmp_list1 = []
    tmp_list2 = []
    csv_data = read_csv_seed()
    vacancies_data = data[0]["data"]
    vacancy_rate_data = data[1]["data"]
    #print(vacancies_data)
    vacancies_result = []
    vacancy_rate_result = []
    #print(f"{len(data[0]['data'])} , {len(data[1]['data'])}")

    if len(data[0]['data']) == len(data[1]['data']) :
        for i in vacancies_data:
            tmp_vacancies_month_year = convert_date(int(i[0]))
            tmp_vacancies = i[1] / csv_data[csv_i][1]
            if (i[1] % csv_data[csv_i][1] != 0):
                err_flag = "has remainder"
            else:
                err_flag = ""
            #print(f"{postcode},{tmp_vacancies_month_year},{tmp_vacancies}")
            tmp_str = f"{postcode},{tmp_vacancies_month_year},{i[1]},{tmp_vacancies},{csv_data[csv_i][1]},{err_flag}"
            tmp_list = tmp_str.split(",")
            tmp_list1.append(tmp_list)
            csv_i += 1
        for j in vacancy_rate_data:
            tmp_vacancy_rate_month_year = convert_date(int(j[0]))
            tmp_vacancy_rate = j[1] / csv_data[csv_j][1]
            tmp_str = f"{postcode},{tmp_vacancy_rate_month_year},{j[1]},{tmp_vacancy_rate}"
            tmp_list = tmp_str.split(",")
            tmp_list2.append(tmp_list)
            csv_j += 1
            #print(f"{postcode},{tmp_vacancy_rate_month_year},{tmp_vacancy_rate}")

        result = [x + y[2:] for x, y in zip(tmp_list1, tmp_list2)]
        result = [sublist[:4] + sublist[6:] + sublist[4:6] for sublist in result]
        print(f"{postcode}, {csv_i} of records")
        # save the list to sqlite
        save_data_to_db(result)

def convert_date(unix_time):
    timestamp = unix_time / 1000  # Divide by 1000 to convert milliseconds to seconds
    dt = datetime.datetime.fromtimestamp(timestamp)
    local_dt = datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc).astimezone()

    formatted_time = local_dt.strftime("%b %Y")
    #print(formatted_time)
    return formatted_time

def read_csv_seed():
    # Read the CSV data into a list
    csv_data = []
    with open('hidden_seed.csv', 'r') as f:
        reader = csv.reader(f, skipinitialspace=True, delimiter=',')
        # next(reader)  # Skip the header row
        for row in reader:
            # Parse the month-year field from the CSV data
            month_year_str = row[0]
            value = int(row[1])
            # month_year = datetime.datetime.strptime(month_year_str, '%b %Y')
            csv_data.append((month_year_str, value))

    # print(csv_data)
    return csv_data

def save_data_to_db(data_list):
    # create connection and cursor
    conn = sqlite3.connect('database.db', timeout=10)
    c = conn.cursor()
    count = 0
    # # create table if it doesn't exist
    # c.execute('''CREATE TABLE IF NOT EXISTS properties
    #              (id INTEGER PRIMARY KEY AUTOINCREMENT,
    #               postcode TEXT,
    #               month_year TIMESTAMP,
    #               pro_type TEXT,
    #               pro_num INTEGER,
    #               pro_total TEXT,
    #               pro_total_num INTEGER,
    #               create_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    for data in data_list:
        postcode = data[0]
        month_year = data[1]

        c.execute("SELECT * FROM sqm_vacancy_rate WHERE postcode = ? AND month_year = ?", (postcode, month_year))
        existing_data = c.fetchone()

        if existing_data is None:
            vacancies_raw = data[2]
            vacancies_num = data[3]
            vacancy_rate_raw = data[4]
            vacancy_rate_percentage = data[5]
            hidden_seed = data[6]
            err_flag = data[7]
            c.execute('''INSERT INTO sqm_vacancy_rate (postcode, month_year, vacancies_raw, vacancies_num, vacancy_rate_raw, vacancy_rate_percentage, hidden_seed, err_flag)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
            postcode, month_year, vacancies_raw, vacancies_num, vacancy_rate_raw, vacancy_rate_percentage, hidden_seed, err_flag))
            count += 1
    # # insert data into table
    # for data in data_list:
    #     postcode = data[0]
    #     month_year = data[1]
    #     vacancies_raw = data[2]
    #     vacancies_num = data[3]
    #     vacancy_rate_raw = data[4]
    #     vacancy_rate_percentage = data[5]
    #     hidden_seed = data[6]
    #     err_flag = data[7]
    #
    #     # # convert month_year to TIMESTAMP format
    #     # datetime_str = f"{month_year} 01 00:00:00"
    #     # datetime_obj = datetime.datetime.strptime(datetime_str, "%b %Y %d %H:%M:%S")
    #
    #     c.execute('''INSERT INTO sqm_vacancy_rate (postcode, month_year, vacancies_raw, vacancies_num, vacancy_rate_raw, vacancy_rate_percentage, hidden_seed, err_flag)
    #                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (postcode, month_year, vacancies_raw, vacancies_num, vacancy_rate_raw, vacancy_rate_percentage, hidden_seed, err_flag))

    # commit changes and close connection
    conn.commit()
    conn.close()
    save_postcode(postcode)
    #print(f"{postcode} wrote to database")
    print(f"{postcode}, {count} record wrote to database")

def save_postcode(postcode):
    with open("postcode_done.txt", "a") as file:
        file.write(postcode + "\n")
    #print(f"Postcode {postcode} saved to postcode_done.txt")

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
    #postcode_list = ['6100','2100',]
    list_done = postcode_done()
    #print(list_done)
    postcode_list = [item for item in postcode_list if item not in list_done]
    list_nodata = postcode_nodata()
    postcode_list = [item for item in postcode_list if item not in list_nodata]
    print(f"Number of postcode to do: {len(postcode_list)}")
    # split the list
    num = 3
    postcode_split = split_list(postcode_list, num)
    #print(postcode_split)

    #free_proxies = get_free_proxies()
    #print(free_proxies)

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

#get_sqm_data()
read_json()