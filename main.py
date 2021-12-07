import sqlite3
import os
import matplotlib.pyplot as plt 
import numpy as np
import unittest
import json
import requests
import pandas as pd

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
    api_access = '7e4b80cb2b51728a63998f57c1c23629'
    stocks_url = 'https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&limit=120&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy'
    api3_result = requests.get('https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&limit=120&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy').text
    data3 = json.loads(api3_result)
    return data3

#stocks table 
def set_up_stocks(data3, cur,conn):
    cur.execute('DROP TABLE IF EXISTS "Stocks Data"')
    cur.execute('CREATE TABLE "Stocks Data"("name" INTEGER PRIMARY KEY, "highest_price" INTEGER, "lowest_price" INTEGER,"trading_volume" INTEGER, "transaction_number" INTEGER)')

    newdata3 = data3 
    count3 = 1 
    for i in newdata3: 
        id3 = count3
        highest_price = i['results']['h']
        lowest_price = i['results']['l']
        trading_volume = i['results']['v']
        transaction_number = i['results']['t']
        cur.execute('INSERT INTO "Stocks Data"(name,highest_price,lowest_price,trading_volume,transaction_number)VALUES(?,?,?,?,?)',(id3,highest_price,lowest_price,trading_volume,transaction_number))
        count3 = count3 + 1 
    conn.commit()
    
    

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

    '''set_up_covid(covid_data, cur, conn, 27, 51)
    set_up_covid(covid_data, cur, conn, 52, 76)
    set_up_covid(covid_data, cur, conn, 77, 101)
    set_up_covid(covid_data, cur, conn, 102, 126)
    set_up_covid(covid_data, cur, conn, 127, 176)
    set_up_covid(covid_data, cur, conn, 177, 201)
    set_up_covid(covid_data, cur, conn, 202, 226)
    set_up_covid(covid_data, cur, conn, 227, 251)
    set_up_covid(covid_data, cur, conn, 252, 276)
    set_up_covid(covid_data, cur, conn, 277, 301)
    set_up_covid(covid_data, cur, conn, 302, 326)
    set_up_covid(covid_data, cur, conn, 327, 351)
    set_up_covid(covid_data, cur, conn, 352, 376)
    set_up_covid(covid_data, cur, conn, 377, 401)
    set_up_covid(covid_data, cur, conn, 426, 451)
    set_up_covid(covid_data, cur, conn, 452, 476)
    set_up_covid(covid_data, cur, conn, 477, 501)'''

    # Create stock table
    stock_data = stock_api()
    set_up_stocks(stock_data, cur, conn)

    # Create Bitcoin table
    #bitcoin_data = bitcoin_api()
    #set_up_bitcoin(bitcoin_data, cur, conn)

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
