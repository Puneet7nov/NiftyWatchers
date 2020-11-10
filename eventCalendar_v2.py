############## This script is used for extracting Event Calendar ############################################################################
################### The script extracts all the upcoming corporate events such as Results, Dividend payouts #################################
######################### Version 1 released by Puneet Srivastava in September 2020 #########################################################

import pandas as pd
from datetime import datetime
from datetime import date
import pyodbc
from requests.exceptions import ConnectionError
import logging
import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import timedelta
import os
## import library with all the DB functions for this project
from NiftyWatcherDBLayer import *

logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

curdate = date.today() + timedelta(6)
curdate = curdate.strftime('%d-%m-%Y')

##### the below url_proxy is used as a hack to go across website access restrictions
url_proxy = 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar'

url = 'https://www.nseindia.com/api/event-calendar?index=equities&from_date=%s&to_date=%s' %(curdate,curdate)
session = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Mobile Safari/537.36'}

######### Check if the url is accessible #######################
try:
    session.get(url_proxy, headers=headers)
    page = session.get(url, headers=headers)

except ConnectionError:
    logging.info("calling url " + url)
    logging.critical("The event calendar api is not accessible in eventCalendar.py script")


################################# Function to make API call ###############################################################

def event_calendar_call():
    try:
        csv_json = open('downloaded_event_calendar.json', 'wb')
        csv_json.write(page.content)
        
    except IOError as e:
        logging.critical("Error in event_calendar_call function eventCalendar.py in script")
        logging.critical(e)    
    except OSError as er:
        logging.critical("There is some issue with event_calendar_call in eventCalendar.py script")
        logging.critical(er)
        
    finally:
        csv_json.close()
        
################ The below function json data to csv #####################################################################

def event_calendar_json_to_csv():
    try:
        with open('downloaded_event_calendar.json') as json_file:
            json_data = json.load(json_file)
        
        event_csv_fmt = open('downloaded_event_calendar.csv', 'w')
        csv_writer = csv.writer(event_csv_fmt)
        count = 0
        for i in json_data:
            if count == 0:
                header = i.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(i.values())
    
    except IOError as e:
        logging.critical(e)    
    except OSError as er:
        logging.critical(er)
    except EmptyDataError as err:
        logging.critical("The downloaded_event_calendar.csv file is empty")
        logging.critical(err)
    except:
        logging.critical("There is some issue with event_calendar_json_to_csv in eventCalendar.py script")
    
    finally:
        event_csv_fmt.close()


################ The below function pushes the csv data to DB ############################################################

def event_calendar_csv_to_db():

    insert_table_name = 'events_calendar'
    try:
        df_to_select = pd.read_csv('downloaded_event_calendar.csv')
        df_to_insert = df_to_select[['symbol', 'company', 'purpose', 'bm_desc', 'date']]
        df_to_insert['date'] = pd.to_datetime(df_to_select['date']).astype(str)
        df_to_insert['company'] = df_to_select['company'].str.replace("'","")
        df_to_insert['purpose'] = df_to_select['purpose'].str.replace("'","")
        df_to_insert['bm_desc'] = df_to_select['bm_desc'].str.replace("'","")
        #print(df_to_insert.head(20))
        
        if df_to_insert.empty == False:
            insertDataFrameFunc(df_to_insert, insert_table_name)
            #pass
        else:
            logging.error("The Event Calendar dataframe could not be computed, 0 rows inserted")        
           
    except AttributeError as e:
        logging.error("Error in eventCalendar.py script insider_trade_csv_to_db function")
        logging.critical(e)


################ Function to clean up the intermediatery files ##########################################################

def file_cleanup():

    try:
        for fname in ['downloaded_event_calendar.json', 'downloaded_event_calendar.csv']:
        
            if os.path.exists(fname):
                os.remove(fname)
            else:
                logging.error("Unable to remove downloaded_event_calendar.json file")
    except IOError as e:
        logging.critical(e)    
    except OSError as er:
        logging.critical(er)   

######################### Main Function #################################################################################
#########################################################################################################################

def main():    

    logging.info("Starting to execute eventCalendar.py")

    try:
        if (page.status_code == 200):
            event_calendar_call()
            event_calendar_json_to_csv()
            event_calendar_csv_to_db()
            file_cleanup()
        else:
            logging.critical("The API call is Incorrect as the page status_code is not equal to 200 in eventCalendar.py script")
        logging.info("Ending execution of eventCalendar.py")

    except NameError as e:
        logging.error(e)

################################################################################################################

if __name__ == "__main__":
    main()