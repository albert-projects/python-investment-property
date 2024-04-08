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
    def __init__(self, pcode, free_proxies):
        #threading.Thread.__init__(self)
        super(ScrapeThread, self).__init__()
        self.pcode = pcode
        self.free_proxies = free_proxies

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
        #driver = webdriver.Chrome(options=options)

        while retry_count < max_retries:
            # Randomly proxy pick an IP address and port
            r_proxy = random.choice(self.free_proxies)
            ip_address = r_proxy['IP Address']
            port = r_proxy['Port']
            proxy = f"{ip_address}:{port}"
            #options.add_argument('--proxy-server={}'.format(proxy))
            #print(proxy)
            #driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
            #driver.delete_all_cookies()
            driver = webdriver.Chrome(options=options)
            retry_count += 1
            success = 1
            # Navigate to the webpage
            url = f'https://sqmresearch.com.au/total-property-listings.php?postcode={self.pcode}&t=1&hu=1'
            driver.get(url)

            try:
                # Wait for the element to be present
                element_present = EC.presence_of_element_located((By.ID, "hichartcontainerSOMhus"))
                WebDriverWait(driver, 5).until(element_present)

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
            # found the hichartcontainerSOMhus and go ahead
            #print("Element found.")
            #print(driver)
            #print(f"running {self.pcode}")
            #time.sleep(.2)
            # Wait for the webpage to load completely
            #wait = WebDriverWait(driver, 5)
            #wait.until(EC.presence_of_element_located((By.ID, 'hichartcontainerSOMhus')))
            #get the title the postcode
            #print(driver.page_source)
            #soup = BeautifulSoup(driver.page_source, 'html.parser')
            #print(f"running {self.pcode}")
            #print(driver.page_source)

            # # switch to use bs4 for web scrape
            # # Define the headers
            # headers = {
            #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            #     'Accept-Language': 'en-US,en;q=0.9',
            #     'Accept-Encoding': 'gzip, deflate, br',
            #     'Connection': 'keep-alive',
            #     'Upgrade-Insecure-Requests': '1'
            # }
            # response = requests.get(f'https://sqmresearch.com.au/total-property-listings.php?postcode={self.pcode}&t=1&hu=1', headers=headers)
            # print(response.text)
            # # get the title the postcode
            # soup = BeautifulSoup(response.text, 'html.parser')

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            #check if the postcode have data
            div = soup.find('div', {'id': 'hichartcontainerSOMhus'})
            #print(driver.page_source)

            if div and div.text.strip():
                title = soup.find('div', {'id': 'plainInside'}).find('h1').text.strip()
                #print(title)
                pcode = soup.find('div', {'id': 'plainInside'}).find('h2').text.strip()
                # Separate the string using split() method and get the last element <h2>Postcode 6210</h2>
                postcode = pcode.split()[-1]
                # Trim the string using slice notation and get the last 4 digits
                postcode = postcode[-4:]
                # Verify if the postcode is a number
                if not postcode.isnumeric():
                    # print("Invalid postcode")
                    postcode = 0000
                #print(f"{title}, {postcode}")

                # find the encoding script
                # find all script tags inside div with id plainInside
                scripts = soup.select('#plainInside script')
                #print(scripts)
                # loop through the script tags and find the one containing data starting with eval
                for script in scripts:
                     if script.string and script.string.strip().startswith('eval'):
                        # extract the data string from the eval statement
                        data = script.string.strip()
                        data = data.replace("eval", "")
                        decoded_data = js2py.eval_js(data)
                        #print(decoded_data)

                        # Regular expression to match the data
                        pattern = r"series:\[\{name:'Units',data:[^}]*\},\{name:'Houses',data:[^}]*\}\]"
                        result = re.findall(pattern, decoded_data)
                        if result:
                            json_data = result[0].replace("series:[{name:'Units',data:",'[{"name":"Units","data":')
                            json_data = json_data.replace(",{name:'Houses',data:", ',{"name":"Houses","data":')
                            #print(json_data)
                            load_decoded_json(json_data, postcode)
                        # else:
                        #     print("No match found.")

                # # find the class name in the loop
                # find_classes = ['rect.highcharts-point.highcharts-color-0',
                #               'rect.highcharts-point.highcharts-color-1'
                #              ]
                #
                # for item in find_classes:
                #     # Find the elements that trigger the dynamic data when hovered over
                #     elements = driver.find_elements(By.CSS_SELECTOR, item)
                #
                #     # Loop through the elements and simulate the mouse hover action
                #     for element in elements:
                #         actions = ActionChains(driver)
                #         actions.move_to_element(element).perform()
                #         #time.sleep(1)  # Wait for the dynamic data to load
                #
                #         # Wait for the dynamic data to load
                #         count = 0
                #         while True:
                #             count += 1
                #             soup = BeautifulSoup(driver.page_source, 'html.parser')
                #             tooltip = soup.select_one('g.highcharts-label.highcharts-tooltip')
                #             if tooltip:
                #                 tspan_tags = tooltip.find_all('tspan')
                #                 if len(tspan_tags) == 3:  # make sure we have all three spans
                #                     month_year = tspan_tags[0].text
                #                     ptype = tspan_tags[1].text
                #                     total = tspan_tags[2].text
                #                     if (month_year != prev_month_year) or (ptype != prev_ptype) or (total != prev_total):
                #                         break
                #             #time.sleep(0.5)
                #             print("still old data, reload again")
                #             if count >= 20:
                #                 print("tried too many times, quit loop")
                #                 break
                #
                #         #convert the string to number
                #         parts = ptype.split(":")
                #         pro_type = parts[0]
                #         pro_num = int(parts[1].replace(",", ""))
                #         parts = total.split(":")
                #         pro_total = parts[0]
                #         pro_total_num = int(parts[1].replace(",", ""))
                #
                #
                #         prev_month_year = month_year
                #         prev_ptype = pro_num
                #         prev_total = pro_total_num
                #         print(f"{postcode},{month_year},{pro_type},{pro_num},{pro_total_num}")
                #         tmp_str = f"{postcode},{month_year},{pro_type},{pro_num},{pro_total_num}"
                #         tmp_list = tmp_str.split(",")
                #         data_list.append(tmp_list)

                # save the list to sqlite
                #save_data_to_db(data_list)
            elif div:
                empty_list = [[self.pcode,"Apr 2023",0,0,0,0,0,0,"[0]","NoData"]]
                save_data_to_db(empty_list)

        # Close the web driver
        driver.quit()

def load_decoded_json(json_data, postcode):

    data = json.loads(json_data)
    factors_dict = {}
    csv_data = read_csv_seed()
    csv_ind = 0
    data_list = []

    for units_section in data:
        if units_section['name'] == 'Units':
            for units_dataset in units_section['data']:
                if units_dataset[0] in [dataset[0] for section in data if section['name'] == 'Houses' for dataset in
                                        section['data']]:
                    for houses_section in data:
                        if houses_section['name'] == 'Houses':
                            for houses_dataset in houses_section['data']:
                                if houses_dataset[0] == units_dataset[0]:
                                    units_value = units_dataset[1]
                                    houses_value = houses_dataset[1]
                                    units_factors = set(
                                        [i for i in range(10, units_value + 1) if units_value % i == 0 and i <= 99])
                                    houses_factors = set(
                                        [i for i in range(10, houses_value + 1) if houses_value % i == 0 and i <= 99])
                                    common_factors = sorted(list(units_factors.intersection(houses_factors)))
                                    common_num = common_factors
                                    month_year = units_dataset[0]
                                    unit_raw = units_dataset[1]
                                    house_raw = houses_dataset[1]
                                    if units_value == 0 or houses_value == 0:
                                        #print(csv_data[csv_ind][1])
                                        #print(f"Postcode: ;{postcode}; Date: ;{convert_date(month_year)}; num: ;[0]; unit: ;{unit_raw};{unit_raw/csv_data[csv_ind][1]}; house: ;{house_raw};{house_raw/csv_data[csv_ind][1]}")
                                        if (unit_raw % csv_data[csv_ind][1] != 0) or (house_raw % csv_data[csv_ind][1] != 0) :
                                            err_flag = "has remainder"
                                        else:
                                            err_flag = ""

                                        tmp_str = f"{postcode},{convert_date(month_year)},{unit_raw},{unit_raw/csv_data[csv_ind][1]},{house_raw},{house_raw/csv_data[csv_ind][1]},{unit_raw/csv_data[csv_ind][1]+house_raw/csv_data[csv_ind][1]},{csv_data[csv_ind][1]},[0],{err_flag}"
                                        tmp_list = tmp_str.split(",")
                                        data_list.append(tmp_list)
                                        csv_ind += 1
                                        #print(month_year)
                                        # for k in csv_data:
                                        #     if k[0] == str(convert_date(month_year)):
                                        #         print(k[1])
                                        #         print(f"Postcode: ;{postcode}; Date: ;{k[0]}; num: ;[0]; unit: ;{unit_raw};{unit_raw / k[1]}; house: ;{house_raw};{house_raw / k[1]}")
                                    if len(common_factors) > 0:
                                        factors_dict[units_dataset[0]] = {'factors': common_factors,
                                                                          'Houses': common_factors}
                                        #print(csv_data[csv_ind][1])
                                        #print(f"Postcode: ;{postcode}; Date: ;{convert_date(month_year)}; num: ;{common_num}; unit: ;{unit_raw};{unit_raw / csv_data[csv_ind][1]}; house: ;{house_raw};{house_raw / csv_data[csv_ind][1]}")
                                        if (unit_raw % csv_data[csv_ind][1] != 0) or (house_raw % csv_data[csv_ind][1] != 0):
                                            err_flag = "has remainder"
                                        else:
                                            err_flag = ""
                                        common_num = str(common_num).replace(",","|")
                                        common_num = common_num.replace(" ", "")
                                        tmp_str = f"{postcode},{convert_date(month_year)},{unit_raw},{unit_raw/csv_data[csv_ind][1]},{house_raw},{house_raw/csv_data[csv_ind][1]},{unit_raw/csv_data[csv_ind][1] + house_raw/csv_data[csv_ind][1]},{csv_data[csv_ind][1]},{common_num},{err_flag}"
                                        tmp_list = tmp_str.split(",")
                                        data_list.append(tmp_list)
                                        csv_ind += 1
                                        #print(f"data_list {len(data_list)}")
                                        # for k in csv_data:
                                        #     if k[0] == str(convert_date(month_year)):
                                        #         print(k[1])
                                        #         print(f"Postcode: ;{postcode}; Date: ;{convert_date(month_year)}; num: ;{common_num}; unit: ;{unit_raw};{unit_raw / k[1]}; house: ;{house_raw};{house_raw / k[1]}")

    print(f"{postcode}, {csv_ind} of records")
    # save the list to sqlite
    save_data_to_db(data_list)

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

    # # insert data into table
    # for data in data_list:
    #     postcode = data[0]
    #     month_year = data[1]
    #     unit_raw = data[2]
    #     unit_num = data[3]
    #     house_raw = data[4]
    #     house_num = data[5]
    #     property_total = data[6]
    #     hidden_seed = data[7]
    #     common_factors = data[8]
    #     err_flag = data[9]
    #
    #     # # convert month_year to TIMESTAMP format
    #     # datetime_str = f"{month_year} 01 00:00:00"
    #     # datetime_obj = datetime.datetime.strptime(datetime_str, "%b %Y %d %H:%M:%S")
    #
    #     c.execute('''INSERT INTO sqm_total_property_listing (postcode, month_year, unit_raw, unit_num, house_raw, house_num, property_total, hidden_seed, common_factors, err_flag)
    #                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (postcode, month_year, unit_raw, unit_num, house_raw, house_num, property_total, hidden_seed, common_factors, err_flag))

    for data in data_list:
        postcode = data[0]
        month_year = data[1]

        c.execute("SELECT * FROM sqm_total_property_listing WHERE postcode = ? AND month_year = ?",
                  (postcode, month_year))
        existing_data = c.fetchone()

        if existing_data is None:
            unit_raw = data[2]
            unit_num = data[3]
            house_raw = data[4]
            house_num = data[5]
            property_total = data[6]
            hidden_seed = data[7]
            common_factors = data[8]
            err_flag = data[9]

            c.execute('''INSERT INTO sqm_total_property_listing (postcode, month_year, unit_raw, unit_num, house_raw, house_num, property_total, hidden_seed, common_factors, err_flag)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (postcode, month_year, unit_raw, unit_num, house_raw, house_num, property_total, hidden_seed, common_factors, err_flag))

            count += 1
            #print(f"{postcode} wrote to database")

    # commit changes and close connection
    conn.commit()
    conn.close()
    save_postcode(postcode)
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
    #postcode_list = ['2046','3529','5012','3158','6000','4169']
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

    free_proxies = get_free_proxies()
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
            t = ScrapeThread(postcode, free_proxies)
            t.start()
            threads.append(t)
            #print(f"Done {postcode}")

        for t in threads:
            t.join()
        print(f"Thread {thread_ind} done")
        thread_ind += 1

def remove_old_rows(ip_list, threshold_seconds):
    filtered_list = []
    for entry in ip_list:
        last_checked_str = entry['Last Checked']
        seconds_ago = get_seconds_ago(last_checked_str)
        if seconds_ago <= threshold_seconds:
            filtered_list.append(entry)
    return filtered_list

def get_seconds_ago(time_str):
    if 'hour' in time_str:
        hours = int(time_str.split(' hour')[0])
    else:
        hours = 0

    if 'min' in time_str:
        minutes = int(time_str.split(' min')[0].split()[-1])
    else:
        minutes = 0

    if 'sec' in time_str:
        seconds = int(time_str.split(' sec')[0].split()[-1])
    else:
        seconds = 0

    return hours * 3600 + minutes * 60 + seconds

def get_free_proxies():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get('https://sslproxies.org')

    table = driver.find_element(By.TAG_NAME, 'table')
    thead = table.find_element(By.TAG_NAME, 'thead').find_elements(By.TAG_NAME, 'th')
    tbody = table.find_element(By.TAG_NAME, 'tbody').find_elements(By.TAG_NAME, 'tr')

    headers = []
    for th in thead:
        headers.append(th.text.strip())

    proxies = []
    for tr in tbody:
        proxy_data = {}
        tds = tr.find_elements(By.TAG_NAME, 'td')
        for i in range(len(headers)):
            proxy_data[headers[i]] = tds[i].text.strip()
        proxies.append(proxy_data)

    driver.quit()
    threshold_seconds = 1800
    filtered_list = remove_old_rows(proxies, threshold_seconds)
    #print(filtered_list)
    return filtered_list

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