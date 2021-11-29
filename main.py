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

# Test google API
# Goal is to get all of our API info into JS format
def get_api1():
    response_API = requests.get('https://gmail.googleapis.com/$discovery/rest?version=v1')
    data = response_API.text
    parse_json = json.loads(data)
    info = parse_json['description']
    print("Info about API:\n", info)
    key = parse_json['parameters']['key']['description']
    print("\nDescription about the key:\n",key)
    pass

# Get COVID-19 API data in JS:
def covid_api():
    response_API = requests.get('https://api.covidtracking.com/v2/us/daily.json')
    data = response_API.text
    parse_json = json.loads(data)
    info = parse_json['data'][0]['date']
    print("\n\nFirst date included:\n", info)
    pass

# Get stock API data in JS:
def stock_api():
    pass

# Get Yelp API data in JS: (to see how many businesses are in covid heavy areas?)




# idk just included in case we need it?
def readDataFromFile(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

def main():
    covid_api()

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
