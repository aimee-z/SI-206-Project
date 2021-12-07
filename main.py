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
    


# Get COVID-19 API data in JSON
def covid_api():
    # Request for historic US COVID data
    response_API = requests.get('https://api.covidtracking.com/v2/us/daily.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create COVID table
def covid_table(data, cur, conn):
    cur.execute('DROP TABLE IF EXISTS "COVID Data"')
    cur.execute('CREATE TABLE "COVID Data"("id" INTEGER PRIMARY KEY, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

# Compile COVID JSON data into database
def set_up_covid(data, cur, conn, start, end):
    newdata = data['data']
    newdata.sort(key = lambda x:x['date'])

    count = start
    row_count = 1
    for i in newdata:
        id = count
        date = i['date']
        total_cases = i['cases']['total']['value']
        case_percent_population = i['cases']['total']['calculated']['population_percent']
        change_in_population = i['cases']['total']['calculated']['change_from_prior_day']
        hospitalized = i['outcomes']['hospitalized']['currently']['value']
        deaths = i['outcomes']['death']['total']['value']
        cur.execute('INSERT INTO "Covid Data" (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths) VALUES (?,?,?,?,?,?,?)', (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths))
        count = count + 1
        if count == end:
            break
    conn.commit()
    pass



# Get BitcoinAverage API data in JSON
def bitcoin_api():
    base_url = 'https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/historical?'
    param = {}
    param['start'] = '2020-01-13'
    param['end'] = '2021-03-07'
    param['limit'] = 1
    api2_result = requests.get(base_url, params=param)
    data2 = api2_result.json()
    #data2_text = data2.text
    #parse_json2 = json.loads(data2_text)
    return data2

# Compile BitcoinAverage JSON data into database
def set_up_bitcoin(data2, cur, conn):
    cur.execute('DROP TABLE IF EXISTS "Bitcoin Data"')
    cur.execute('CREATE TABLE "Bitcoin Data"("id" INTEGER PRIMARY KEY, "time_open" TEXT, "time_close" INTEGER, "open" INTEGER, "high" INTEGER, "low" INTEGER, "close" INTEGER)')

    newdata2 = data2
    count2 = 1
    for i in newdata2:
        id2 = count2
        time_open = i['time_open']
        time_close = i['time_close']
        bitcoin_open = i['open']
        bitcoin_high = i['high']
        bitcoin_low = i['low']
        bitcoin_close = i['close']
        cur.execute('INSERT INTO "Bitcoin Data" (id, time_open, time_close, open, high, low, close) VALUES(?,?,?,?,?,?,?)', (id2, time_open, time_close, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close))
        count2 = count2 + 1
    conn.commit()
    pass

# BELOW: RANDOM CODE IN CASE WE NEED IT

# Get stock API data in JS:
def stock_api():
    params = {'access_key': '68deeb901ca7f891572effb4145fe675'}

    api_result = requests.get('https://api.marketstack.com/v1/tickers/aapl/eod', params)

    api_response = api_result.json()

    #print(api_result)
    '''params = {'access_key': '68deeb901ca7f891572effb4145fe675', 'date_from': '2020-01-13', 'date_to': '2021-03-07'}
    response_API = requests.get('https://api.marketstack.com/v1/tickers/AMZN/eod', params)
    data = response_API.text
    parse_json = json.loads(data)
    print(parse_json)
    return parse_json'''
    pass
    data = api_response.text
    parse_json = json.loads(data)
    info = parse_json['description']
    print("Info about API:\n", info)
    key = parse_json['parameters']['key']['description']
    print("\nDescription about the key:\n",key)
    pass 

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
    covid_table(covid_data, cur, conn)
    set_up_covid(covid_data, cur, conn, 1, 26)
    set_up_covid(covid_data, cur, conn, 27, 51)
    set_up_covid(covid_data, cur, conn, 52, 76)
    set_up_covid(covid_data, cur, conn, 77, 101)
    
    # Create AMZN stock table
    # stock_api()

    # Create Bitcoin table
    #bitcoin_data = bitcoin_api()
    #set_up_bitcoin(bitcoin_data, cur, conn)

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
