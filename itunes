from datetime import datetime, date, timedelta
import requests
import json
from polygon import RESTClient
import os
import csv
import numpy as np
import sqlite3
import time

zero = 0
locID = 2459115 #NYC #for weather api
key = "OO6kdLUPSMrVQMTjxBODpM92v244QDai"

def get_stock_date():

    start_date = date(2018, 1, 25)
    end_date = date(2018, 2, 25)

    stock_date_list = []
    delta = timedelta(days=1)
    while start_date <= end_date:
        stock_date = start_date.strftime("%Y-%m-%d")
        stock_date_list.append(stock_date)
        start_date += delta
    return stock_date_list

def get_stock_data(stock, date):
        '''In this function a stock symbol and date is inputted, and it 
        returns the percent change'''

        request_url = 'https://api.polygon.io/v1/open-close/' + stock + '/' + date + '?unadjusted=true&apiKey=' + key
        r = requests.get(request_url)
        j = (r.json)
        data = r.text
        dict_list = json.loads(data)
        
        if dict_list['status'] == 'OK': 
            opening = dict_list['open']
            close = dict_list['close']
            percent_change = (float(close) - float(opening))/float(opening)
            return percent_change
        
        if dict_list['status'] == 'NOT FOUND': #this is for weekends and holidays; no change on these days. you can also try skipping these
            percent_change = zero
            return percent_change
        
        if dict_list['status'] == 'ERROR': # after 5 times sourcing api.polygon.io locks you out for a minute, so we wait a minute and try again.
            time.sleep(62)
            pass
            
def getAverage(cur,conn):
    '''This function takes in cur and conn in order to find the average of each
     column in the weather table of the database. It creates a CSV that contains
      each variable and the month’s average. This function returns nothing'''

    cur.execute("SELECT AVG(MaxTemp), AVG(MinTemp), AVG(Humidity), AVG(AirPressure), AVG(WindSpeed) FROM Weather")
    rs = cur.fetchall()
    conn.commit()
    outfile = open('./averageTemp.csv','w')
    writer=csv.writer(outfile)
    writer.writerow(['Average Max Temp', 'Average Min Temp', 'Average Humidity', 'Average Air Pressure', 'Average Wind Speed'])
    writer.writerows(rs)
    outfile.close()

def getAverageStocks(cur,conn):
    '''This function takes in cur and conn in order to find the average of each column in the 
    stocks table of the database. It creates a CSV that contains the variable and the month’s 
    percent change average. This function returns nothing'''

    cur.execute("SELECT AVG(percent_change) FROM Stocks")
    rs = cur.fetchall()
    conn.commit()
    outfile = open('./averageStocks.csv','w')
    writer=csv.writer(outfile)
    writer.writerow(['Average Percent Change'])
    writer.writerows(rs)
    outfile.close()

def setUpDatabase(db_name):
    '''This function creates a database by taking in a database name, as a string,
     and then establishing a path and storing the path to the directory. It returns
      the cursor and connection to the database.'''

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def add_to_db(date, percent_change, open, stock, cur, conn):
    '''The add_to_db function inserts the data into the connected database in a table called Stock'''
    cur.execute("CREATE TABLE IF NOT EXISTS Stocks (date TEXT PRIMARY KEY, percent_change INTEGER, open INTEGER, stock TEXT)")
    cur.execute("SELECT * FROM Stocks WHERE Stocks.date=?", (date,)) #see if info for this date is in db
    data = cur.fetchall()
    # print(data)
    if len(data) == 0: #if this date has not been put in the db yet
        cur.execute("INSERT INTO Stocks (date, percent_change, stock) VALUES (?,?,?)",(date, percent_change, stock))
        conn.commit()
        print('--------------------')
        print("inserting info to database for: " + str(date))
        print("percent change is: " + str(percent_change))
        return True
    else: 
        print(date + " is already in database.")
        return False



def add_weather_to_db(date, locID, year, month, day, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Weather (Date TEXT PRIMARY KEY, MaxTemp INTEGER, MinTemp INTEGER, Humidity INTEGER, AirPressure INTEGER, WindSpeed INTEGER, WeatherState TEXT)")
    cur.execute("SELECT * FROM Weather WHERE Weather.Date=?", (date,)) #see if info for this date is in db
    data = cur.fetchall()
    if len(data) == 0: #if this date has not been put in the db yet
        url = 'https://www.metaweather.com/api/location/' + str(locID) + '/' + str(year) + '/' + str(month) + '/' + str(day) + '/'   
        r = requests.get(url)
        j = (r.json())

        maxtemp = (j[0].get('max_temp'))
        mintemp = (j[0].get('min_temp'))
        humidity = (j[0].get('humidity'))
        airpressure = (j[0].get('air_pressure'))
        windespeed = (j[0].get('wind_speed'))
        weatherstate = (j[0].get('weather_state_name'))
        date = (j[0].get('applicable_date'))

        print("MaxTemp: " + str(maxtemp))
        cur.execute("INSERT INTO Weather (Date, MaxTemp, MinTemp, Humidity, AirPressure, WindSpeed, WeatherState) VALUES (?,?,?,?,?,?,?)",(date, maxtemp, mintemp, humidity, airpressure,windespeed,weatherstate))
        conn.commit()
        return True
    else:
        return False

def join_db(cur, conn): 

    cur.execute("SELECT * FROM Stocks INNER JOIN Weather ON Weather.Date=Stocks.date")
    join_list = cur.fetchall()
    return join_list

def write(filename, join_list):

    fields = ['Date', 'PercentChange', 'Stock', 'Date', 'MaxTemp', 'MinTemp', 'Humidity', 'AirPressure', 'WindSpeed', 'WeatherState']
    rows = join_list
    with open(filename, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(fields)
        # writing the data rows
        csvwriter.writerows(rows)


def main():
    '''Main picks a stock symbol like NDAQ and compares it on a day-by-day basis with weather'''


    stock = 'NDAQ' #pick whatever stock you want here
    database = 'test1000.db'
    filename = 'final_project.csv'
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    count = 0 #to ensure we only call the api 25 times per run
    dates = get_stock_date()

    for date in dates:
        percent_change = get_stock_data(stock, date)
        open = get_stock_data(stock,date)
        if count < 25: 
            add_to_db(date, percent_change, open, stock, cur, conn)
            year = date[:4]
            month = date[5:7]
            day = date[8:]
            add_weather_to_db(date, locID, year, month, day, cur, conn)
        count = count + 1
            

    join_list = join_db(cur,conn)
    write(filename, join_list)

if __name__ == '__main__':
    main()