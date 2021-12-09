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
    ''' Takes in database name (string) as input. Returns database cursor and connection as outputs.'''
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# Get National COVID-19 API data in JSON
def covid_api():
    ''' Takes in no inputs. Uses requests module to get historical daily COVID-19 statistics from COVID-19 API. 
    Returns COVID-19 data for date range in JSON formatting.'''
    response_API = requests.get('https://api.covidtracking.com/v2/us/daily.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create National COVID table
def covid_table(data, cur, conn):
    '''Takes in COVID-19 JSON formatted data from covid_api(), database cursor and connector as inputs. 
    Returns nothing. Creates a new table called COVID_Data.'''
    # cur.execute('DROP TABLE IF EXISTS "COVID_Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

# Compile National COVID JSON data into database
def set_up_covid(data, cur, conn, start):
    '''Takes in COVID-19 JSON formatted data from covid_api(), database cursor and connector as inputs. Returns nothing. 
    Indexes date, total cases, percentage of the population with positive case, hospitalizations, and deaths from COVID-19 data
    into COVID_Data table.'''
    newdata = data['data']
    newdata.sort(key = lambda x:x['date'])

    cur.execute('SELECT * FROM Covid_Data WHERE Covid_Data.sequential_day = ?', (start,))
    ifday = cur.fetchall()
    if len(ifday) == 0:
        day_num = start
        date = newdata[start]['date']
        total_cases = newdata[start]['cases']['total']['value']
        case_percent_population = newdata[start]['cases']['total']['calculated']['population_percent']
        change_in_population = newdata[start]['cases']['total']['calculated']['change_from_prior_day']
        hospitalized = newdata[start]['outcomes']['hospitalized']['currently']['value']
        deaths = newdata[start]['outcomes']['death']['total']['value']
        cur.execute('INSERT OR IGNORE INTO "Covid_Data" (sequential_day, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths) VALUES (?,?,?,?,?,?,?)', (day_num, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths))

    conn.commit()
    pass

# Get NY COVID-19 API data in JSON
def ca_covid_api():
    ''' Takes in no inputs. Uses requests module to get historical daily New York COVID-19 statistics from COVID-19 API. 
    Returns COVID-19 data for date range in JSON formatting.'''
    response_API = requests.get('https://api.covidtracking.com/v2/states/ny/daily/simple.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create NY COVID table
def ca_covid_table(data, cur, conn):
    '''Takes in COVID-19 JSON formatted data from ca_covid_api(), database cursor and connector as inputs. 
    Returns nothing. Creates a new table called NY_COVID_Data.'''
    #cur.execute('DROP TABLE IF EXISTS "NY_COVID_Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "NY_COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "deaths" INTEGER)')

# Compile NY COVID JSON data into database
def set_up_ca_covid(data, cur, conn, start):
    '''Takes in COVID-19 JSON formatted data from ca_covid_api(), database cursor and connector as inputs. Returns nothing. 
    Indexes date, total cases from New York COVID-19 data into NY_COVID_Data table.'''
    newdata = data['data']
    newdata.sort(key = lambda x:x['date'])
    
    cur.execute('SELECT * FROM NY_Covid_Data WHERE NY_Covid_Data.sequential_day = ?', (start,))
    ifday = cur.fetchall()
    if len(ifday) == 0:
        new = start-49
        date = newdata[new]['date']
        #cur.execute('SELECT sequential_day FROM Covid_Data WHERE date = ?', (date,))
        #seq_day = cur.fetchone()[0]
        seq_day = start
        total_cases = newdata[new]['cases']['total']
        deaths = newdata[new]['outcomes']['death']['total']
        cur.execute('INSERT OR IGNORE INTO "NY_Covid_Data" (sequential_day, date, total_cases, deaths) VALUES (?,?,?,?)', (seq_day, date, total_cases, deaths))

    conn.commit()

# Join NY COVID and National COVID Data 
def join_tables(cur,conn):
    '''Takes in database cursor and connector as inputs. Uses JOIN to return a list of tuples in the format 
    (NY cases, date, National cases) where date is the same.'''
    cur.execute("SELECT Covid_Data.sequential_day, Covid_Data.date, Covid_Data.total_cases FROM Covid_Data LEFT JOIN NY_COVID_Data ON NY_COVID_Data.sequential_day = Covid_Data.sequential_day")
    results = cur.fetchall()
    conn.commit()
    print(results)
    return results

# Get Bitcoin JSON data
def bitcoin_api():
    base_url = 'https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/historical?'
    param = {}
    param['start'] = '2020-01-13'
    param['end'] = '2021-03-07'
    param['limit'] = 1
    api2_result = requests.get(base_url, params=param)
    data2 = api2_result.json()
    return data2

# Create Bitcoin Table
def bitcoin_table(data2, cur, conn):
    #cur.execute('DROP TABLE IF EXISTS "Bitcoin_Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "Bitcoin_Data"("sequential_day" INTEGER PRIMARY KEY, "date" TEXT, "open" INTEGER, "high" INTEGER, "low" INTEGER, "close" INTEGER)')

# Compile Bitcoin JSON data into database
def set_up_bitcoin(data2, cur, conn, start):

    newdata2 = data2
    cur.execute('SELECT * FROM Bitcoin_Data WHERE Bitcoin_Data.sequential_day = ?', (start,))

    ifday = cur.fetchall()    
    if len(ifday) == 0:           
        time_open = newdata2[start]['time_open'][0:10]
        cur.execute('SELECT sequential_day FROM Covid_Data WHERE date = ?', (time_open,))
        seq_day = cur.fetchone()[0]
        bitcoin_open = newdata2[start]['open']
        bitcoin_high = newdata2[start]['high']
        print(bitcoin_high)
        bitcoin_low = newdata2[start]['low']
        bitcoin_close = newdata2[start]['close']
        cur.execute('INSERT OR IGNORE INTO "Bitcoin_Data" (sequential_day, date, open, high, low, close) VALUES(?,?,?,?,?,?)', (seq_day, time_open, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close))

    conn.commit()
    pass

def main():

    cur, conn = setUpDatabase('project.db')

    # Create COVID table
    covid_data = covid_api()
    covid_table(covid_data, cur, conn)
    cur.execute('SELECT * FROM Covid_Data')
    info = cur.fetchall()
    if len(info) < 25:
        for i in range(49, 74):
            set_up_covid(covid_data, cur, conn, i)
    elif 25 <= len(info) < 50:
        for i in range(74, 99):
            set_up_covid(covid_data, cur, conn, i)
    elif 50 <= len(info) < 75:
        for i in range(99, 124):
            set_up_covid(covid_data, cur, conn, i)
    else:
        for i in range(124, 149):
            set_up_covid(covid_data, cur, conn, i)

    # Create NY COVID table
    ca_covid_data = ca_covid_api()
    ca_covid_table(ca_covid_data, cur, conn)
    cur.execute('SELECT * FROM NY_COVID_Data')
    info = cur.fetchall()
    if len(info) < 25:
        for i in range(49, 74):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    elif 25 <= len(info) < 50:
        for i in range(74, 99):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    elif 50 <= len(info) < 75:
        for i in range(99, 124):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    else:
        for i in range(124, 149):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    
    # Create Bitcoin Table
    bitcoin_data = bitcoin_api()
    bitcoin_table(bitcoin_data, cur, conn)
    cur.execute('SELECT * FROM Bitcoin_Data')
    info = cur.fetchall()
    if len(info) < 25:
        for i in range(49, 74):
            set_up_bitcoin(bitcoin_data, cur, conn, i)
    elif 25 <= len(info) < 50:
        for i in range(74, 99):
            set_up_bitcoin(bitcoin_data, cur, conn, i)
    elif 50 <= len(info) < 75:
        for i in range(99, 124):
            set_up_bitcoin(bitcoin_data, cur, conn, i)
    else:
        for i in range(124, 149):
            set_up_bitcoin(bitcoin_data, cur, conn, i)

    # Join National and NY COVID Data
    calculations = join_tables(cur, conn)

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)


