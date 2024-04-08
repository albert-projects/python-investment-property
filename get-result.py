#!/usr/bin/env python3

# Module Imports
import time
import sqlite3
import pandas as pd
from datetime import datetime
import csv

# Connect to the SQLite database
conn = sqlite3.connect('database.db', timeout=10)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Define the filter values as variables
month_year = 'Sep 2023'
pv_month_year = 'Oct 2023'
year = '2021'
property_type = 'House' # House or Unit

# Define the SQL query with placeholders for the variables
sql_query = """
SELECT sqm_established_properties.postcode, sqm_total_property_listing.month_year, sqm_established_properties.type, ROUND(CAST(sqm_total_property_listing.house_num AS REAL) / sqm_established_properties.established_prop * 100, 2) || '%' AS percentage_on_market
FROM sqm_total_property_listing
JOIN sqm_established_properties ON sqm_total_property_listing.postcode = sqm_established_properties.postcode
WHERE sqm_total_property_listing.month_year = ?
  AND sqm_established_properties.year = ?
  AND sqm_established_properties.type = ?
  AND ROUND(CAST(sqm_total_property_listing.house_num AS REAL) / sqm_established_properties.established_prop * 100, 2) < 1.5
ORDER BY sqm_total_property_listing.postcode ASC
"""

# Execute the query with the filter values as parameters
cursor.execute(sql_query, (month_year, year, property_type))

# Fetch all the rows returned by the query
rows = cursor.fetchall()

# Create a list to store the query results
result_list = []
result_vacancy_rate = []
result_rented = []
result_pv = []

# Process and populate the query results into the list
for row in rows:
    result_list.append(row)

# Define the SQL query for sqm_vacancy_rate table
sql_query_vacancy = """
SELECT postcode, month_year, vacancy_rate_percentage
FROM sqm_vacancy_rate
WHERE vacancy_rate_percentage < 2
  AND vacancy_rate_percentage <> 0
  AND month_year = ?
"""

# Execute the query for sqm_vacancy_rate table
cursor.execute(sql_query_vacancy, (month_year,))

# Fetch all the rows returned by the query
vacancy_rows = cursor.fetchall()

# Process and populate the query results into the list
for vacancy_row in vacancy_rows:
    result_vacancy_rate.append(vacancy_row)

# Create a list to store the joined results
joined_result_list = []

# Join the result_list and vacancy_result_list based on the same postcode and month_year
for result_item in result_list:
    for vacancy_item in result_vacancy_rate:
        if result_item[0] == vacancy_item[0] and result_item[1] == vacancy_item[1]:
            joined_result_list.append(result_item + vacancy_item)

# Remove the 5th and 6th columns from each item in the joined_result_list
final_result_list = [item[:4] + item[6:] for item in joined_result_list]
# Round the numbers in the 5th column and add a '%' symbol
final_result_list1 = [(item[0], item[1], item[2], item[3], f"{round(item[4], 2)}%") for item in final_result_list]


# Define the SQL query for sqm_vacancy_rate table
sql_renter_proportion = """
select postcode, year, rented from sqm_occupant_type
where rented <> '0'
and rented <= '35'
and year = ?
"""
# Execute the query for sqm_vacancy_rate table
cursor.execute(sql_renter_proportion, (year,))

# Fetch all the rows returned by the query
rented_rows = cursor.fetchall()

# Process and populate the query results into the list
for rented_row in rented_rows:
    result_rented.append(rented_row)

# # Print the final result list
# print(f"rented Results: {len(result_rented)}")
# for item in result_rented:
#     print(item)

# Create a new list to store the joined data
joined_list = []

# Iterate over the first list and join the matching items
for item1 in final_result_list1:
    postcode = item1[0]
    for item2 in result_rented:
        if item2[0] == postcode:
            joined_list.append(item1 + item2)
            break

# Remove the 6th and 7th columns from each item in the joined_result_list
final_result_list2 = [item[:5] + item[7:] for item in joined_list]
final_result_list3 = [(item[0], item[1], item[2], item[3], item[4], f"{round(item[5], 2)}%") for item in final_result_list2]



# Define the SQL query for PV days on market and vendor discount
# sql_pv = """
# SELECT
#     t.postcode,
#     ROUND(AVG(
#         CASE
#             WHEN median_value LIKE '$%' || '%K' THEN CAST(REPLACE(median_value, '$', '') AS REAL) * 1000
#             WHEN median_value LIKE '$%' || '%M' THEN CAST(REPLACE(median_value, '$', '') AS REAL) * 1000000
# 			WHEN median_value = '0' THEN NULL
#             ELSE NULL
#         END
#     ), 2) AS average_median_value,
#     ROUND(AVG(
#         CASE
#             WHEN properties_sold <> '0' THEN CAST(properties_sold AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_properties_sold,
#     ROUND(AVG(
#         CASE
#             WHEN median_rent <> '0' THEN CAST(REPLACE(REPLACE(median_rent, 'pw', ''), '$', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_rent,
#     ROUND(AVG(
#         CASE
#             WHEN median_gross_yield <> '0' THEN CAST(REPLACE(median_gross_yield, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_gross_yield,
#     ROUND(AVG(
#         CASE
#             WHEN average_days_on_market <> '0' THEN CAST(average_days_on_market AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_average_days_on_market,
#     ROUND(AVG(
#         CASE
#             WHEN average_vendor_discount <> '0' THEN CAST(REPLACE(average_vendor_discount, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_average_vendor_discount,
#     ROUND(AVG(
#         CASE
#             WHEN median_price_change_1yr <> '0' THEN CAST(REPLACE(median_price_change_1yr, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_price_change_1yr
# FROM
#     pv_market_trends AS t
# WHERE
#     t.postcode IN (
#         SELECT
#             postcode
#         FROM
#             pv_market_trends
#         WHERE
#             average_days_on_market <> '0'
#             AND average_vendor_discount <> '0'
#             AND CAST(average_days_on_market AS INTEGER) < 60
#             AND CAST(REPLACE(average_vendor_discount, '%', '') AS REAL) > -5
#             AND data_time = ?
#     )
# GROUP BY
#     t.postcode
# HAVING
# 	avg_average_days_on_market < 60
# 	AND avg_average_vendor_discount > -5
#
# """

# remark the old sql query, coz no need to avg the value
# sql_pv = """
# SELECT
#     t.postcode, t.data_time,
#     ROUND(AVG(
#         CASE
#             WHEN median_value LIKE '$%' || '%K' THEN CAST(REPLACE(median_value, '$', '') AS REAL) * 1000
#             WHEN median_value LIKE '$%' || '%M' THEN CAST(REPLACE(median_value, '$', '') AS REAL) * 1000000
# 			WHEN median_value = '0' THEN NULL
#             ELSE NULL
#         END
#     ), 2) AS average_median_value,
#     ROUND(AVG(
#         CASE
#             WHEN properties_sold <> '0' THEN CAST(properties_sold AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_properties_sold,
#     ROUND(AVG(
#         CASE
#             WHEN median_rent <> '0' THEN CAST(REPLACE(REPLACE(median_rent, 'pw', ''), '$', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_rent,
#     ROUND(AVG(
#         CASE
#             WHEN median_gross_yield <> '0' THEN CAST(REPLACE(median_gross_yield, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_gross_yield,
#     ROUND(AVG(
#         CASE
#             WHEN average_days_on_market <> '0' THEN CAST(average_days_on_market AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_average_days_on_market,
#     ROUND(AVG(
#         CASE
#             WHEN average_vendor_discount <> '0' THEN CAST(REPLACE(average_vendor_discount, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_average_vendor_discount,
#     ROUND(AVG(
#         CASE
#             WHEN median_price_change_1yr <> '0' THEN CAST(REPLACE(median_price_change_1yr, '%', '') AS REAL)
#             ELSE NULL
#         END
#     ), 2) AS avg_median_price_change_1yr
# FROM
#     pv_market_trends AS t
# WHERE
#     t.postcode IN (
#         SELECT
#             postcode
#         FROM
#             pv_market_trends
#         WHERE
#             average_days_on_market <> '0'
#             AND average_vendor_discount <> '0'
#             AND CAST(average_days_on_market AS INTEGER) < 60
#             AND CAST(REPLACE(average_vendor_discount, '%', '') AS REAL) > -5
#             AND data_time = ?
#     )
# GROUP BY
#     t.postcode, t.data_time
# HAVING
# 	avg_average_days_on_market < 60
# 	AND avg_average_vendor_discount > -5
# 	AND data_time = ?
# """
sql_pv = """
SELECT
    postcode, suburb, median_value, properties_sold, median_rent, median_gross_yield, average_days_on_market, average_vendor_discount, median_price_change_1yr, data_time
FROM
    pv_market_trends 
WHERE
    average_days_on_market <> '0'
    AND average_vendor_discount <> '0'
	AND average_days_on_market <> '0'
    AND CAST(average_days_on_market AS INTEGER) < 60
    AND CAST(REPLACE(average_vendor_discount, '%', '') AS INTEGER) > -5
    AND data_time = ?
"""

# Execute the query for sqm_vacancy_rate table
cursor.execute(sql_pv, (pv_month_year,))

# Fetch all the rows returned by the query
pv_rows = cursor.fetchall()

# Process and populate the query results into the list
for pv_row in pv_rows:
    result_pv.append(pv_row)
#print(final_result_list3)
#print(result_pv)

# Create a new list to store the joined data
#joined_list2 = []

# Iterate over the first list and join the matching items
# for item1 in final_result_list3:
#     postcode = item1[0]
#     for item2 in result_pv:
#         if item2[0] == postcode:
#             joined_list2.append(item1 + item2)
#             break

result_list_2 = []
for record1 in final_result_list3:
    postcode = record1[0]
    matching_records = [record2 for record2 in result_pv if record2[0] == postcode]

    if matching_records:
        for record2 in matching_records:
            new_record = record1 + record2[1:]
            result_list_2.append(new_record)
    else:
        result_list_2.append(record1)

joined_list2 = [record for record in result_list_2 if len(record) > 6]
#joined_list3 = [record[:-1] for record in joined_list2]
joined_list3 = [
    record[:7] + (float(record[7].replace('$', '').replace('K', '')) * 1000 if 'K' in record[7] else float(record[7].replace('$', '').replace('M', '')) * 1000000,) + record[8:]
    for record in joined_list2
]
final_result_list4 = [record[:1] + (record[6],) + record[1:6] + record[7:] for record in joined_list3]

# Remove the 6th and 7th columns from each item in the joined_result_list
#final_result_list4 = [tuple(item[:6] + item[7:]) for item in joined_list2]

# # Print the final result list
# print(f"Final Results: {len(final_result_list4)}")
# for item in final_result_list4:
#     print(item)

headers = ['postcode', 'Suburb', 'Date', 'Type', 'Percentage on Market(<1.5)', 'Vacancy rate(<2)',
           'Renter proportion(<35)', 'Median Value', 'Properties Sold', 'Median Rent',
           'Median Gross Yield', 'Days on Market(<60)', 'Vendor Discount(<5)', 'Median Price Change', 'PV Date']

# Create a DataFrame from the list with the specified headers
df = pd.DataFrame(final_result_list4, columns=headers)

# Generate the current datetime string
current_datetime = datetime.now().strftime("%Y-%m-%d_%H%M%S")

# Construct the filename with the current datetime
filename = f"data_{current_datetime}.xlsx"
#excel_writer = pd.ExcelWriter(filename, engine='xlsxwriter')
# Save the DataFrame to the Excel writer in a sheet named 'Sheet1'
#df.to_excel(excel_writer, sheet_name='Result', index=False)

# Create the DataFrame
headers2 = ['postcode', 'Date', 'Type', 'Percentage on Market(<1.5)', 'Vacancy rate(<2)', 'Renter proportion(<35)']
df2 = pd.DataFrame(final_result_list3, columns=headers2)
# Save the second DataFrame to the Excel writer in a sheet named 'Sheet2'
#df2.to_excel(excel_writer, sheet_name='SQM_Data', index=False)

try:
    # Save the DataFrame to an Excel file
    excel_writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df.to_excel(excel_writer, sheet_name='Result', index=False)
    df2.to_excel(excel_writer, sheet_name='SQM_Data', index=False)
    #excel_writer.save()
    excel_writer.close()
    #df.to_excel(filename, index=False)
    print(f"Data saved to {filename}")
except Exception as e:
    print(f"Error, excel file hasn't been saved. {e}")

# Close the cursor and database connection
cursor.close()
conn.close()