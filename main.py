import sqlite3
import os
import matplotlib.pyplot as plt
import numpy as np
import unittest
import json
import requests
#hello

## Team Name: Women in Stem
## Team Members: Aimee Zheng, Sage Pei, Marina Sun


# Create database
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
    

# Get COVID-19 API data in JS
def covid_api():
    # Request for historic US COVID data
    response_API = requests.get('https://api.covidtracking.com/v2/us/daily.json')
    data = response_API.text
    parse_json = json.loads(data)
    # Check first date to make sure it works lol
    # info = parse_json['data'][0]['date']
    # print("\n\nFirst date included:\n", info)
    return parse_json

# Compile COVID JS data into database
# data here would be covid_api()
def set_up_covid(data, cur, con):
    setUpDatabase('project.db')
    cur.execute("CREATE TABLE Covid (id INTEGER PRIMARY KEY, title TEXT)")

    pass

# Get stock API data in JS:
def stock_api():
    pass

# Get EventBrite API data in JS: (to see how many events happen during COVID spikes?)




# idk just included in case we need it?
def readDataFromFile(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

def main():
    set_up_covid()

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
