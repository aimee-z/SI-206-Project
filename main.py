import sqlite3
import os
import matplotlib.pyplot as plt
import numpy as np
import unittest
import json
import requests

## Team Name: Women in Stem
## Team Members: Aimee Zheng, Sage Pei, Marina Sun


# Create project database
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
    return parse_json

# Compile COVID JS data into database
def set_up_covid(data, cur, conn):
    # is there something suspicious abt dropping table if exist? read project specs
    cur.execute('DROP TABLE IF EXISTS "COVID Data"')
    cur.execute('CREATE TABLE "COVID Data"("id" INTEGER PRIMARY KEY, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

    newdata = data['data']
    count = 1
    for i in newdata:
        id = count
        date = i['date']
        # may need to trim down to 341 dates (4/2/20), too many dates for other missing data points
        total_cases = i['cases']['total']['value']
        case_percent_population = i['cases']['total']['calculated']['population_percent']
        change_in_population = i['cases']['total']['calculated']['change_from_prior_day']
        hospitalized = i['outcomes']['hospitalized']['currently']['value']
        deaths = i['outcomes']['death']['total']['value']
        cur.execute('INSERT INTO "Covid Data" (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths) VALUES (?,?,?,?,?,?,?)', (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths))
        count = count + 1
    conn.commit()
    pass

# Get stock API data in JS:
def stock_api():
    params = {'access_key': '68deeb901ca7f891572effb4145fe675'}

    api_result = requests.get('https://api.marketstack.com/v1/tickers/aapl/eod', params)

    api_response = api_result.json()

    print(api_result)
    '''params = {'access_key': '68deeb901ca7f891572effb4145fe675', 'date_from': '2020-01-13', 'date_to': '2021-03-07'}
    response_API = requests.get('https://api.marketstack.com/v1/tickers/AMZN/eod', params)
    data = response_API.text
    parse_json = json.loads(data)
    print(parse_json)
    return parse_json'''
    pass

# Get BitcoinAverage API data in JS: (to see how many events happen during COVID spikes?)
def bitcoin_api():
    api_key = 'YjM3M2MwOTNmZjUxNDIyMzgwYjVlZmRkZTQzYzU4OGM'
    base_url = 'https://apiv2.bitcoinaverage.com/indices/'
    param = {}
    param['symbol_set'] = 'global'
    param['symbol'] = 'BTCUSD'
    param['period'] = 'day'
    param['format'] = 'json'
    api2_result = requests.get(base_url, params=param)
    print(api2_result[0])



# idk just included in case we need it?
def readDataFromFile(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

def main():

    cur, conn = setUpDatabase('project.db')

    # Create COVID table
    covid_data = covid_api()
    set_up_covid(covid_data, cur, conn)

    # Create AMZN stock table
    stock_api()

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
