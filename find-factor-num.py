import json
import datetime
import csv

def convert_date(unix_time):
    timestamp = unix_time / 1000  # Divide by 1000 to convert milliseconds to seconds
    dt = datetime.datetime.fromtimestamp(timestamp)
    local_dt = datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc).astimezone()

    formatted_time = local_dt.strftime("%b %Y")
    #print(formatted_time)
    return formatted_time

# Read the CSV data into a list
csv_data = []
with open('hidden_seed.csv', 'r') as f:
    reader = csv.reader(f, skipinitialspace=True, delimiter=',')
    #next(reader)  # Skip the header row
    for row in reader:
        # Parse the month-year field from the CSV data
        month_year_str = row[0]
        value = int(row[1])
        #month_year = datetime.datetime.strptime(month_year_str, '%b %Y')
        csv_data.append((month_year_str, value))

#print(csv_data)

with open('2000.json') as f:
    data = json.load(f)

factors_dict = {}

for units_section in data:
    if units_section['name'] == 'Units':
        for units_dataset in units_section['data']:
            if units_dataset[0] in [dataset[0] for section in data if section['name'] == 'Houses' for dataset in section['data']]:
                for houses_section in data:
                    if houses_section['name'] == 'Houses':
                        for houses_dataset in houses_section['data']:
                            if houses_dataset[0] == units_dataset[0]:
                                units_value = units_dataset[1]
                                houses_value = houses_dataset[1]
                                units_factors = set([i for i in range(10, units_value+1) if units_value % i == 0 and i <= 99])
                                houses_factors = set([i for i in range(10, houses_value+1) if houses_value % i == 0 and i <= 99])
                                common_factors = sorted(list(units_factors.intersection(houses_factors)))
                                if len(common_factors) > 0:
                                    factors_dict[units_dataset[0]] = {'factors': common_factors, 'Houses': common_factors}
                                    common_num = common_factors
                                    month_year = units_dataset[0]
                                    unit_raw = units_dataset[1]
                                    house_raw = houses_dataset[1]
                                    for k in csv_data:
                                        if k[0] == str(convert_date(month_year)):
                                            print(k[1])
                                            print(f"Date: {convert_date(month_year)}, num: {common_num}, unit: {unit_raw}/{unit_raw/k[1]}, house: {house_raw}/{house_raw/k[1]}")

#print(json.dumps(factors_dict))

