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



# Get National COVID-19 API data in JSON
def covid_api():
    response_API = requests.get('https://api.covidtracking.com/v2/us/daily.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create National COVID table
def covid_table(data, cur, conn):
    # cur.execute('DROP TABLE IF EXISTS "COVID_Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

# Compile National COVID JSON data into database
def set_up_covid(data, cur, conn, start):
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
    response_API = requests.get('https://api.covidtracking.com/v2/states/ny/daily/simple.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create NY COVID table
def ca_covid_table(data, cur, conn):
    #cur.execute('DROP TABLE IF EXISTS "NY_COVID_Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "NY_COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "deaths" INTEGER)')

# Compile NY COVID JSON data into database
def set_up_ca_covid(data, cur, conn, start):
    newdata = data['data']
    newdata.sort(key = lambda x:x['date'])
    
    cur.execute('SELECT * FROM NY_Covid_Data WHERE NY_Covid_Data.sequential_day = ?', (start,))
    ifday = cur.fetchall()
    if len(ifday) == 0:
        date = newdata[start]['date']
        cur.execute('SELECT sequential_day FROM Covid_Data WHERE date = ?', (date,))
        seq_day = cur.fetchone()[0]
        total_cases = newdata[start]['cases']['total']
        print(total_cases)
        deaths = newdata[start]['outcomes']['death']['total']
        cur.execute('INSERT OR IGNORE INTO "NY_Covid_Data" (sequential_day, date, total_cases, deaths) VALUES (?,?,?,?)', (seq_day, date, total_cases, deaths))

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
'''def bitcoin_table(data2, cur, conn):
    #cur.execute('DROP TABLE IF EXISTS "Bitcoin Data"')
    cur.execute('CREATE TABLE IF NOT EXISTS "Bitcoin Data"("id" INTEGER PRIMARY KEY, "date" TEXT, "open" INTEGER, "high" INTEGER, "low" INTEGER, "close" INTEGER)')

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
    pass'''



'''# Get stock API data in JSON:
def stock_api():
    api_access = '48e36d776c40e470ee12e76e5b9bd8cd'
    stocks_url = 'https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&limit=120&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy'
    api3_result = requests.get('https://api.polygon.io/v2/aggs/ticker/PFE/range/1/day/2020-01-13/2021-03-07?adjusted=true&sort=asc&apiKey=zU1RScZjXgXk3X91fSvGZ8j5gNCUS4xy')
    #data3 = json.loads(api3_result)
    data3 = api3_result.json()
    #print(data3)
    return data3

# Create stocks table 
def stocks_table(data3, cur, conn):
    #stocks_dict = stock_api()
    newdata3 = data3

    
    cur.execute('DROP TABLE IF EXISTS "Stocks_Data"')
    cur.execute('CREATE TABLE "Stocks_Data"("date_id" INTEGER PRIMARY KEY, "Date" TEXT, "highest_price" INTEGER, "lowest_price" INTEGER,"trading_volume" INTEGER, "transaction_number" INTEGER)')
    key = 1 
    
    
    while key < 26: 
        for i in newdata3['results']:
            if key < 26:
                highest_price = i['h']
                lowest_price = i['l']
                trading_volume = i['v']
                transaction_number = i['t']
                #wrong parameter?? 
                cur.execute('INSERT OR IGNORE INTO "Stocks_Data"(date_id,highest_price,lowest_price,trading_volume,transaction_number) VALUES(?,?,?,?,?)',(key,highest_price,lowest_price,trading_volume,transaction_number))
                row_count = cur.rowcount 
                if row_count > 0:
                    key += 1 
                    conn.commit()
                else:
                    break
        continue
 
    #note: maybe try to delete data sets from within the time frame that matches id using a for loop & Select Delete Conditional after loading in data  ? 

    #adding in dates list to stocks_data 
    cur.execute("""SELECT Covid_Data.date FROM Covid_Data JOIN Stocks_Data ON Stocks_Data.date_id = Covid_Data.id;""")
    date_list = cur.fetchall()
    for i in range(1,len(date_list)+1):
        cur.execute('UPDATE Stocks_Data set Date = ? where date_id = ? ',((date_list[i-1][0],i))) 
        #cur.execute('INSERT INTO Stocks_Data(Date) VALUES(?)',((date_list[i-1][0],i)))
        #cur.execute('INSERT INTO Stocks_Data.Date VALUES (?) WHERE date_id (?)', (date_list[i-1][0],i,))
    conn.commit()

    #print(newdata3)

    #25 limiter 
    cur.execute('SELECT * FROM Stocks_Data LIMIT 25')
    selected_list = cur.fetchall()

    #print(selected_list)

    #for i in range(1,len(selected_list)):
   #     cur.execute('DELETE Stocks_Data set Date = ? where date_id = ? ',((selected_list[i-1][0],i))) 
   # conn.commit()'''




#get iTunes Data
def itunes_api():
    #request for 100 itunes movie 
    api1_result = requests.get('https://itunes.apple.com/search?term=movie&limit=100&entity=movie')
    data2 = api1_result.json()
    return data2 
    
    
#create iTunes table
def itunes_table(data, cur, conn):
    cur.execute('DROP TABLE IF EXISTS "iTunes Data"')
    cur.execute('CREATE TABLE "iTunes Data" ("Id" INTEGER PRIMARY KEY, "trackName" TEXT, "releaseDate" TEXT, "trackPrice" INTEGER, "primaryGenreName" TEXT, "contentAdvisoryRating" TEXT)')

#compile iTunes data into database 
def set_up_itunes(data, cur, conn):
    count  = 1 
    for i in data['results']:
        id = count
        trackName = i['trackName']
        releaseDate = i['releaseDate']
        trackPrice = i['trackPrice']
        primaryGenreName = i['primaryGenreName']
        contentAdvisoryRating = i['contentAdvisoryRating']
        cur.execute('INSERT INTO "iTunes Data"(Id,trackName,releaseDate,trackPrice,primaryGenreName,contentAdvisoryRating) VALUES (?,?,?,?,?,?)',(count,trackName,releaseDate,trackPrice,primaryGenreName,contentAdvisoryRating))
        count = count + 1 
    conn.commit()



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

    # Create Cali COVID table
    ca_covid_data = ca_covid_api()
    ca_covid_table(ca_covid_data, cur, conn)
    cur.execute('SELECT * FROM NY_COVID_Data')
    info = cur.fetchall()
    if len(info) < 25:
        for i in range(0, 25):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    elif 25 <= len(info) < 50:
        for i in range(24, 50):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    elif 50 <= len(info) < 75:
        for i in range(50, 75):
            set_up_ca_covid(ca_covid_data, cur, conn, i)
    else:
        for i in range(74, 100):
            set_up_ca_covid(ca_covid_data, cur, conn, i)

    # Create stock table
    '''Stocks_Data = stock_api()
    stocks_table(Stocks_Data,cur,conn)
    
    set_up_stocks(Stocks_Data, cur, conn, '2020-01-13', '2020-02-06')
    set_up_stocks(Stocks_Data, cur, conn,'2020-02-06', '2020-03-01')
    set_up_stocks(Stocks_Data, cur, conn, '2020-10-01', '2020-10-25')
    set_up_stocks(Stocks_Data, cur, conn, '2020-10-26', '2020-11-19')'''
    
    # Create Bitcoin Table
    '''bitcoin_data = bitcoin_api()
    bitcoin_table(bitcoin_data, cur, conn)
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-01-13', '2020-02-06')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-02-06', '2020-03-01')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-10-01', '2020-10-25')
    set_up_bitcoin(bitcoin_data, cur, conn, '2020-10-26', '2020-11-19')'''

    #Create IMDB Table 
    #IMDB_Data = IMDB_api()
    #IMDB_table(IMDB_Data, cur, conn)

    #Create iTunes table 
    itunes_data = itunes_api()
    itunes_table(itunes_data,cur,conn)
    set_up_itunes(itunes_data, cur, conn)
    

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)


