import sqlite3
import os
import matplotlib.pyplot as plt 
import matplotlib.ticker as plticker
import numpy as np
import unittest
import json
from numpy.core.fromnumeric import size
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
    cur.execute('CREATE TABLE IF NOT EXISTS "COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "case_percent_population" REAL, "change_in_population" INTEGER, "hospitalized" INTEGER, "deaths" INTEGER)')

# Compile National COVID JSON data into database
def set_up_covid(data, cur, conn, start):
    '''Takes in COVID-19 JSON formatted data from covid_api(), database cursor and connector, and start value (integer) as inputs. Returns nothing. 
    Indexes date, total cases, percentage of the population with positive case, hospitalizations, and deaths from COVID-19 data
    into COVID_Data table.
    
    Uses start value to limit amount of data to 25 collected/stored at a time '''
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
def ny_covid_api():
    ''' Takes in no inputs. Uses requests module to get historical daily New York COVID-19 statistics from COVID-19 API. 
    Returns COVID-19 data for date range in JSON formatting.'''
    response_API = requests.get('https://api.covidtracking.com/v2/states/ny/daily/simple.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

# Create NY COVID table
def ny_covid_table(data, cur, conn):
    '''Takes in COVID-19 JSON formatted data from ny_covid_api(), database cursor and connector as inputs. 
    Returns nothing. Creates a new table called NY_COVID_Data.'''
    cur.execute('CREATE TABLE IF NOT EXISTS "NY_COVID_Data"("sequential_day" INTEGER, "date" TEXT, "total_cases" INTEGER, "deaths" INTEGER)')

# Compile NY COVID JSON data into database
def set_up_ny_covid(data, cur, conn, start):
    '''Takes in COVID-19 JSON formatted data from ny_covid_api(), database cursor and connector, and start value (integer) as inputs. Returns nothing. 
    Indexes date, total cases from New York COVID-19 data into NY_COVID_Data table.
    
    Uses start value to limit amount of data to 25 collected/stored at a time '''
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
    cur.execute("SELECT NY_COVID_Data.total_cases, Covid_Data.date, Covid_Data.total_cases FROM Covid_Data LEFT JOIN NY_COVID_Data ON NY_COVID_Data.sequential_day = Covid_Data.sequential_day")
    results = cur.fetchall()
    conn.commit()
    return results

# Calculate percentage of national COVID-19 cases that were identified in New York on a given day 
def calculate_ny_nat_cases(lst_of_tups):
    '''Takes in list of tuples from join_tables() and calculates the percentage of national COVID-19 cases that were identified in New York on a given day, and returns it.'''
    percent_lst = []
    for ny_cases, date, nat_cases in lst_of_tups:
        percent_ny = (ny_cases/nat_cases)*100
        percent_lst.append(percent_ny)
    return percent_lst

# Write difference of national to NY cases in a file
def write_diff_to_file(filename, lst_of_tups):
    '''Takes in filename (string) and a list of tuples from join_tables().
    Returns text file ('difference.txt') that writes the difference value of NY/National COVID-19 cases for specified 100 days.'''
    with open(filename, "w", newline="") as fileout:
        fileout.write("Difference of NY COVID-19 cases compared to national cases on a given day:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(lst_of_tups)):
            fileout.write("On {} the difference of NY COVID-19 cases compared to national cases was {} cases.\n".format(lst_of_tups[i][1], (lst_of_tups[i][2]-lst_of_tups[i][0])))
        fileout.close()
    pass

# Calculate total number & average of Covid-19 Cases Nationally on Average between selected dates 
'''Takes in database cursor and connector as inputs. Uses SELECT avg to calculate the average number of cases from Covid_Data and returns the national average'''
def calculate_national_avg (cur,conn):
    nat_covid = ('SELECT avg(total_cases) FROM COVID_Data')
    cur.execute(nat_covid)
    print("The national avg is:")
    print(cur.fetchone()[0])
    conn.commit()
    #average national % calculation
    print("The national avg % is:")
    print((831313.67)/(831313.67+207627.79))

# Calculate total number & average death rates from Covid-19 Nationally on avergae between selected dates 
'''Takes in database cursor and connector as inputs. Uses SELECT avg to calculate the average amount of deaths from Covid_Data and prints the national average'''
def calculate_national_deaths(cur,conn):
    nat_death = ('SELECT avg(deaths) FROM COVID_DATA')
    cur.execute(nat_death)
    print("The national death avg is:")
    print(cur.fetchone()[0])
    conn.commit()
    # average national death % calculation
    print("The national death avg % is:")
    print((44934.48)/(44934.48+14216.471264367816))
    

# Calculate total number & average  of Coviid-19 Cases from NY on Average between selected dates
'''Takes in database cursor and connector as inputs. Uses SELECT avg to calculate the average number of cases from NY_COVID_DATA and prints both the NY average'''
def calculate_ny_avg (cur,conn):
    ny_covid = ('SELECT avg(total_cases) FROM NY_COVID_DATA')
    cur.execute(ny_covid)
    print("The NY avg is:")
    print(cur.fetchone()[0])
    conn.commit()

# Calculate total number & average death rates from Covid-19 in NY on average between selected dates 
'''Takes in database cursor and connector as inputs. Uses SELECT avg to calculate the average amount of deaths from NY_COVID_DATA and prints the NY average '''
def calculate_ny_deaths(cur,conn):
    ny_death = ('SELECT avg(deaths) FROM NY_COVID_DATA')
    cur.execute(ny_death)
    print("The NY death avg is:")
    print(cur.fetchone()[0])
    conn.commit()
    
    
# Write calculation data to file
def write_calculation_to_file(filename, lst_of_tups, percent_lst):
    '''Takes in filename (string), list of tuples from join_tables(), and list of calculated percentages from calculate_ny_nat_cases().
    Returns text file ('calculations.txt') that writes the percentage value of NY/National COVID-19 cases for specified 100 days.'''
    with open(filename, "w", newline="") as fileout:
        fileout.write("Percentage of national COVID-19 cases that were identified in New York on a given day:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(percent_lst)):
            fileout.write("On {} the percent of national COVID-19 cases in New York was {}%.\n".format(lst_of_tups[i][1], round(percent_lst[i],2)))
        fileout.close()
    pass

# Get Bitcoin JSON data
def bitcoin_api():
    '''Takes in no inputs. Uses requests module to get historical daily Bitcoin data from CoinPaprika API. 
    Returns Bitcoin data for date range in JSON formatting.'''
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
    '''Takes in Bitcoin JSON formatted data from bitcoin_api(), database cursor and connector as inputs. 
    Returns nothing. Creates a new table called Bitcoin_Data.'''
    cur.execute('CREATE TABLE IF NOT EXISTS "Bitcoin_Data"("sequential_day" INTEGER PRIMARY KEY, "date" TEXT, "open" INTEGER, "high" INTEGER, "low" INTEGER, "close" INTEGER)')

# Compile Bitcoin JSON data into database
def set_up_bitcoin(data2, cur, conn, start):
    '''Takes in Bitcoin JSON formatted data from bitcoin_api(), database cursor and connector, and a start value (integer) as inputs. Returns nothing. 
    Indexes date, open, high, low, and closing values from Bitcoin data into Bitcoin_Data table. 
    
    Uses start value to limit amount of data to 25 collected/stored at a time '''
    newdata2 = data2
    cur.execute('SELECT * FROM Bitcoin_Data WHERE Bitcoin_Data.sequential_day = ?', (start,))

    ifday = cur.fetchall()    
    if len(ifday) == 0: 
        new = start-49          
        time_open = newdata2[start]['time_open'][0:10]
        cur.execute('SELECT sequential_day FROM Bitcoin_Data WHERE date = ?', (time_open,))
        seq_day = start
        bitcoin_open = newdata2[start]['open']
        bitcoin_high = newdata2[start]['high']
        bitcoin_low = newdata2[start]['low']
        bitcoin_close = newdata2[start]['close']
        cur.execute('INSERT OR IGNORE INTO "Bitcoin_Data" (sequential_day, date, open, high, low, close) VALUES(?,?,?,?,?,?)', (seq_day, time_open, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close))

    conn.commit()
    pass

def bitcoin_graph(lst_of_tups, cur, conn):
    '''Takes in a list of tuples containing the number of NY COVID-19 cases, date, and National COVID cases,
    and the database cursor and connector as inputs. Uses matplotlib to return scatterplot of Bitcoin price ($)
    to National COVID cases (millions) across specified date range.'''
    x_axis_labels = []
    y_axis_labels = []

    cur.execute('SELECT open FROM Bitcoin_Data')
    y_axis = cur.fetchall()
    conn.commit()
    
    for ny_case, date, nat_case in lst_of_tups:
        x_axis_labels.append(nat_case)
    for i in y_axis:
        y_axis_labels.append(i[0])
    
    fig = plt.figure()
    ax = fig.add_subplot()
    plt.scatter(x_axis_labels, y_axis_labels, linestyle='-', marker='*')
    loc = plticker.MultipleLocator(500000)
    ax.xaxis.set_major_locator(loc)
    plt.xlabel('Number of National COVID-19 Cases (millions)')
    plt.ylabel('Price of Bitcoin ($)')
    plt.title("Price of Bitcoin compared to Total National COVID-19 Cases")
    plt.show()


# Create Bar Chart Percentage of NY/NAT Cases
def create_percent_bar(lst_of_tups, percent_lst):
    ''' Takes in a list of tuples containing the number of NY COVID-19 cases, date, and National COVID cases,
    a list of the % of NY COVID cases that made up National COVID cases and the database cursor and connector as inputs.
    Returns a bar chart comparing % of NY COVID cases to National COVID cases.'''
    x_axis_labels = []
    for ny_case, date, nat_case in lst_of_tups:
        x_axis_labels.append(date)
    
    plt.bar(x_axis_labels, percent_lst, color = (0.2, 0.4, 0.6, 0.6))
    plt.xticks(fontsize=6)
    plt.xticks(rotation=45)
    plt.xlabel('Date')
    plt.ylabel('Percent of National COVID-19 Cases in New York')
    plt.title("Bar Chart of Percentage of COVID-19 Cases in New York Compared Nationally")

    plt.show()

# Create Stacked Bar Chart (NY/NAT COVID-19 Cases)
def create_stacked_bar(lst_of_tups):
    '''Takes in a list of tuples containing the number of NY COVID-19 cases, date, and National COVID cases,
    and the database cursor and connector as inputs. Returns a stacked bar chart comparing raw number of NY COVID 
    cases (thousands) to Non-NY COVID cases.'''
    x_axis_labels = []
    ny_cases = []
    not_ny_cases = []

    for ny_case,date,nat_case in lst_of_tups:
        x_axis_labels.append(date)
        ny_cases.append(ny_case/1000)
        not_ny_cases.append(nat_case/1000-ny_case/1000)

    plt.bar(x_axis_labels, ny_cases, color = 'r')
    plt.xticks(fontsize=6)
    plt.xticks(rotation=45)
    plt.bar(x_axis_labels, not_ny_cases, bottom=ny_cases, color = 'b')
    plt.xlabel('Date')
    plt.ylabel('Number of COVID-19 Cases (thousands)')
    plt.title("Stacked Bar Chart of COVID-19 Cases (thousands) in the U.S. by NY Cases and Non-NY Cases")
    plt.legend(['NY COVID-19 Cases', 'Non-NY COVID-19 Cases'])

    plt.show()


# Create Pie Chart (NY Covid-19 Cases vs National Cases)
def create_pie_chart_avg():
    '''Takes in no inputs. Uses average amount of NY and National Covid-19 cases calculated in calculate_national_avg()
    and calculate_ny_avg() to return a pie chart comparing the average cases in NY and nationally over the selected time frame.'''
    labels = 'New York Covid-19 Cases', 'National Covid-19 Cases'
    y = np.array([207627.79,831313.67])
    colors = ['Blue', 'AliceBlue']
    explode = (0.2, 0)

    plt.pie(y, explode=explode, labels=labels, colors=colors, shadow=True, autopct = '%1.2f%%', startangle=140)
    plt.title('NY vs National Covid-19 Cases Distribution')
    plt.axis('equal')
    plt.legend(title = "NY vs National Covid-Cases:")
    plt.show()

# Create Pie Chart for NY Covid-19 Death Rates vs National Death Rates  
def create_pie_chart_deaths():  
    '''Takes in no inputs. Uses average amount of NY and National Covid-19 related deaths calculated in calculate_national_deaths()
    and calculate_ny_deaths() to return a pie chart comparing the average death rates in NY and nationally over the selected time frame.'''
    labels_2 = 'New York Covid-19 Deaths', 'National Covid-19 Deaths'
    y = np.array([14216.471264367816,44934.48])
    colors_2 = ['DarkSalmon', 'Beige']
    explode_2 = (0.2, 0)

    plt.pie(y, explode=explode_2, labels=labels_2, colors=colors_2, shadow=True, autopct = '%1.2f%%',startangle=140)
    plt.title('NY vs National Covid-19 Deaths Distribution')
    plt.legend(title = "NY vs National Covid Related Deaths:")
    plt.axis('equal')
    plt.show()
    
def main():
    '''Takes no inputs and returns nothing. Calls the covid_api(), covid_table(), set_up_covid(), ny_covid_api(), ny_covid_table(), 
    set_up_ny_covid(), join_tables(), bitcoin_api(), bitcoin_table(), and set_up_bitcoin(), set_up_calculations(),write_calculation_to_file(),write_diff_to_file(),
    create_percent_bar(), create_stacked_bar(),bitcoin_graph(), calculate_national_avg(),calculate_national_deaths(),calculate_ny_avg (),
    calculate_ny_deaths(),create_pie_chart_avg(),and create_pie_chart_deaths() functions.
    Limits the amount of to 25 collected/stored at a time.'''

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
    ny_covid_data = ny_covid_api()
    ny_covid_table(ny_covid_data, cur, conn)
    cur.execute('SELECT * FROM NY_COVID_Data')
    info = cur.fetchall()
    if len(info) < 25:
        for i in range(49, 74):
            set_up_ny_covid(ny_covid_data, cur, conn, i)
    elif 25 <= len(info) < 50:
        for i in range(74, 99):
            set_up_ny_covid(ny_covid_data, cur, conn, i)
    elif 50 <= len(info) < 75:
        for i in range(99, 124):
            set_up_ny_covid(ny_covid_data, cur, conn, i)
    else:
        for i in range(124, 149):
            set_up_ny_covid(ny_covid_data, cur, conn, i)
    
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


    # Join National and NY COVID Data, make calculations, and write to file
    set_up_calculations = join_tables(cur, conn)
    calculations = calculate_ny_nat_cases(set_up_calculations)
    write_calculation_to_file("calculations.txt", set_up_calculations, calculations)
    write_diff_to_file("difference.txt", set_up_calculations)

    # Create Bar Chart (Percentage of Nat. COVID-19 Cases in NY)
    create_percent_bar(set_up_calculations, calculations)
    
    # Create Stacked Bar Chart (NY/NAT COVID-19 Cases)
    create_stacked_bar(set_up_calculations)

    # Create Bitcoin file and visualization
    bitcoin_graph(set_up_calculations, cur, conn)

    # Calling calculation for National Cases 
    calculate_national_avg(cur,conn)

    # Calling calculation for National Death Rates  
    calculate_national_deaths(cur,conn)

    # Calling calculation for NY Case
    calculate_ny_avg (cur,conn)

    # Calling calculation for NY Death Rates  
    calculate_ny_deaths(cur,conn)

    # Calling pie chart visualization for average Covid-19 cases (Nationally vs NY) 
    create_pie_chart_avg()

    # Calling pie chart visualization for average Covid-19 related deaths (Nationally vs NY) 
    create_pie_chart_deaths()
    
   
   



if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)