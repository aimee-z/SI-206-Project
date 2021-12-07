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
    cur.execute('DROP TABLE IF EXISTS "COVID_Data"')
    cur.execute('CREATE TABLE "COVID_Data"("id" INTEGER PRIMARY KEY, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

# Compile COVID JSON data into database
def set_up_covid(data, cur, conn, start, end):
    newdata = data['data']
    newdata.sort(key = lambda x:x['date'])

    count = 1
    flag = False

    for i in newdata:
        date = i['date']

        if date < start:
            count = count + 1
            flag = True
            pass

        elif date <= end:
            if flag == True:
                count = count + 1
                flag = False
            id = count
            date = i['date']
            total_cases = i['cases']['total']['value']
            case_percent_population = i['cases']['total']['calculated']['population_percent']
            change_in_population = i['cases']['total']['calculated']['change_from_prior_day']
            hospitalized = i['outcomes']['hospitalized']['currently']['value']
            deaths = i['outcomes']['death']['total']['value']
            cur.execute('INSERT INTO "Covid_Data" (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths) VALUES (?,?,?,?,?,?,?)', (id, date, total_cases, case_percent_population, change_in_population, hospitalized, deaths))
            count = count + 1

        else:
            break
        
    conn.commit()
    pass


# Get Bitcoin JSON data
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

# Create Bitcoin Table
def bitcoin_table(data2, cur, conn):
    cur.execute('DROP TABLE IF EXISTS "Bitcoin Data"')
    cur.execute('CREATE TABLE "Bitcoin Data"("id" INTEGER PRIMARY KEY, "date" TEXT, "open" INTEGER, "high" INTEGER, "low" INTEGER, "close" INTEGER)')

# Compile Bitcoin JSON data into database
def set_up_bitcoin(data2, cur, conn, start2, end2):

    newdata2 = data2
    count2 = 1
    flag2 = False

    for i in newdata2:
        time_open = i['time_open'][0:10]
            
        if time_open < start2:
            count2 = count2 + 1
            flag2 = True
            pass

        elif time_open <= end2:
            if flag2 == True:
                count2 = count2 + 1
                flag2 = False
                    
            id2 = count2
            time_open = i['time_open'][0:10]
            #time_close = i['time_close']
            bitcoin_open = i['open']
            bitcoin_high = i['high']
            bitcoin_low = i['low']
            bitcoin_close = i['close']
            cur.execute('INSERT INTO "Bitcoin Data" (id, date, open, high, low, close) VALUES(?,?,?,?,?,?)', (id2, time_open, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close))
            count2 = count2 + 1

        else:
            break

    conn.commit()
    pass


# Get stock API data in JSON:
def stock_api():
    api_access = '48e36d776c40e470ee12e76e5b9bd8cd'
    stocks_url = 'https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&limit=120&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy'
    api3_result = requests.get('https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy')
    #data3 = json.loads(api3_result)
    data3 = api3_result.json()
    #print(data3)
    return data3

# Create stocks table 
def set_up_stocks(data3, cur,conn):
    cur.execute('DROP TABLE IF EXISTS "Stocks_Data"')
    cur.execute('CREATE TABLE "Stocks_Data"("date_id" INTEGER PRIMARY KEY, "Date" TEXT, "highest_price" INTEGER, "lowest_price" INTEGER,"trading_volume" INTEGER, "transaction_number" INTEGER)')
    
    newdata3 = data3 
    #print(newdata3)
    count3 = 1 
    for i in newdata3['results']: 
        #print(i)
        id3 = count3
        highest_price = i['h']
        lowest_price = i['l']
        trading_volume = i['v']
        transaction_number = i['t']
        cur.execute('INSERT INTO "Stocks_Data"(date_id,highest_price,lowest_price,trading_volume,transaction_number) VALUES(?,?,?,?,?)',(id3,highest_price,lowest_price,trading_volume,transaction_number))
        count3 = count3 + 1 

    cur.execute("""SELECT Covid_Data.date FROM Covid_Data JOIN Stocks_Data ON Stocks_Data.date_id = Covid_Data.id;""")
    
    date_list = cur.fetchall()
    
    for i in range(1,len(date_list)+1):
        cur.execute('UPDATE Stocks_Data set Date = ? where date_id = ? ',((date_list[i-1][0],i))) 
        #cur.execute('INSERT INTO Stocks_Data(Date) VALUES(?)',((date_list[i-1][0],i)))
        #cur.execute('INSERT INTO Stocks_Data.Date VALUES (?) WHERE date_id (?)', (date_list[i-1][0],i,))
    conn.commit()
    

    
    
# RANDOM CODE
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
    set_up_covid(covid_data, cur, conn, '2020-01-13', '2020-02-06')
    set_up_covid(covid_data, cur, conn, '2020-02-06', '2020-03-01')
    set_up_covid(covid_data, cur, conn, '2020-10-01', '2020-10-25')
    set_up_covid(covid_data, cur, conn, '2020-10-26', '2020-11-19')

    # Create stock table
    Stocks_Data = stock_api()
    set_up_stocks(Stocks_Data, cur, conn)
    set_up_stocks(Stocks_Data, cur, conn, '2020-01-13', '2020-02-06')
    set_up_stocks(Stocks_Data, cur, conn, '2020-02-06', '2020-03-01')
    set_up_stocks(Stocks_Data, cur, conn, '2020-10-01', '2020-10-25')
    set_up_stocks(Stocks_Data, cur, conn, '2020-10-26', '2020-11-19')

    # Create Bitcoin Table
    bitcoin_data = bitcoin_api()
    bitcoin_table(bitcoin_data, cur, conn)
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-01-13', '2020-02-06')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-02-06', '2020-03-01')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-10-01', '2020-10-25')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-10-26', '2020-11-19')

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
