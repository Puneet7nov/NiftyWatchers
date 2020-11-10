################# This script is to compute MACD values to understand the trend change and momentum. ######################################################
############################## v1 is released by Puneet Srivastava in October 2020 ########################################################################

import pandas as pd
import pyodbc
import sys
from requests.exceptions import ConnectionError
import logging
import datetime

## import library with all the DB functions for this project
from NiftyWatcherDBLayer import *

logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def dataComputation(df_select, df_compute):
	df_compute['SYMBOL'] = df_select['SYMBOL']
	df_compute['TimeStamp'] = df_select['TimeStamp']
	df_compute['TimeStamp'] = pd.to_datetime(df_compute['TimeStamp'])
	#df_compute['TimeStamp'] = df_compute['TimeStamp'].dt.date
	df_compute['CLOSEPRICE'] = df_select['CLOSEPRICE']
	df_compute['5DayEMA'] = df_select.groupby('SYMBOL')['CLOSEPRICE'].apply(lambda x: x.ewm(span=5, min_periods=5).mean())
	df_compute['9DayEMA'] = df_select.groupby('SYMBOL')['CLOSEPRICE'].apply(lambda x: x.ewm(span=9, min_periods=9).mean())
	df_compute['12DayEMA'] = df_select.groupby('SYMBOL')['CLOSEPRICE'].apply(lambda x: x.ewm(span=12, min_periods=12).mean())
	df_compute['26DayEMA'] = df_select.groupby('SYMBOL')['CLOSEPRICE'].apply(lambda x: x.ewm(span=26, min_periods=26).mean())
	df_compute['35DayEMA'] = df_select.groupby('SYMBOL')['CLOSEPRICE'].apply(lambda x: x.ewm(span=35, min_periods=35).mean())
	df_compute['std_macd'] = df_compute['12DayEMA'] - df_compute['26DayEMA']
	df_compute['fast_macd'] = df_compute['5DayEMA'] - df_compute['35DayEMA']
	df_compute['std_macd_sampling'] = df_compute.groupby('SYMBOL')['std_macd'].apply(lambda x: x.ewm(span=9, min_periods=9).mean())
	df_compute['fast_macd_sampling'] = df_compute.groupby('SYMBOL')['fast_macd'].apply(lambda x: x.ewm(span=5, min_periods=5).mean())
	df_compute['UpMomentum'] = 'No'
	df_compute['DownMomentum'] = 'No'
	#df_compute['std_macd_pct'] = ((df_compute['std_macd'] - df_compute['std_macd_sampling'])/df_compute['std_macd'])*100
	#df_compute['fast_macd_pct'] = ((df_compute['fast_macd'] - df_compute['fast_macd_sampling'])/df_compute['fast_macd'])*100
	return df_compute

################################ Buluk computation on Historical data #########################################################

def historicalDataComputation(table_name, stockList = ""):

	insert_table_name = 'stock_macd'
	select_table_name = table_name
	
	try:
		df_select = selectCompleteTableFunc(select_table_name, where = stockList)
		col_names =  ['SYMBOL', 'TimeStamp', 'CLOSEPRICE', '5DayEMA', '9DayEMA', '12DayEMA', '26DayEMA', '35DayEMA', 'std_macd', 'fast_macd', 'std_macd_sampling', 'fast_macd_sampling', 'UpMomentum', 'DownMomentum']
		df_compute  = pd.DataFrame(columns = col_names)
		if df_select['SYMBOL'].count() > 0:
			df_to_insert = dataComputation(df_select, df_compute)   ### Call dailyDataComputation function
		else:
			logging.error("No data retrived in function historicalDataComputation of MACD_Computation script from table: " + select_table_name)
		
		if df_compute.empty == False:
			df_to_insert['TimeStamp'] = pd.to_datetime(df_to_insert['TimeStamp']).astype(str)
			insertDataFrameFunc(df_to_insert, insert_table_name)   ## Insert computed data into DB
		else:
			logging.error("The macd value cound not be computed in dataComputation function of MACD_Computation script on data retrived from table: " + select_table_name)
	
	except AttributeError as e:
		logging.error("Error in MACD_Computation script dataComputation function on data retrived from table: " + select_table_name)
		logging.critical(e)

############################ Daily computation ###################################################################################

def dailyDataComputation(table_name, stockList = ""):

	insert_table_name = 'stock_macd'
	select_table_name = table_name
	
	try:
		#df_select = pd.read_csv('data.csv')
		df_select = selectForDMAFunc(select_table_name, where = stockList)
		col_names =  ['SYMBOL', 'TimeStamp', 'CLOSEPRICE', '5DayEMA', '9DayEMA', '12DayEMA', '26DayEMA', '35DayEMA', 'std_macd', 'fast_macd', 'std_macd_sampling', 'fast_macd_sampling', 'UpMomentum', 'DownMomentum']
		df_compute  = pd.DataFrame(columns = col_names)
		if df_select['SYMBOL'].count() > 0:
			df_compute = dataComputation(df_select, df_compute)  ### Call dailyDataComputation function
		else:
			logging.error("No data found in function dailyDataComputation of MACD_Computation script on data retrived from table: " + select_table_name)
		
		df_compute_daily = df_compute.groupby('SYMBOL').tail(1)
		
		df_to_insert = stockMomentumCalc(df_compute_daily, df_select) ## Call stockMomentumCalc function for every new date data
		
		if df_to_insert.empty == False:
			df_to_insert['TimeStamp'] = pd.to_datetime(df_to_insert['TimeStamp']).astype(str)
			insertDataFrameFunc(df_to_insert, insert_table_name)  ## Insert computed data into DB
			#print(df_to_insert)
		else:
			logging.error("The macd value cound not be computed in dataComputation function of MACD_Computation script on data retrived from table: " + select_table_name)
	
	except AttributeError as e:
		logging.error("Error in MACD_Computation script dataComputation function")
		logging.critical(e)

############################# Function to do bulk computation on a given symbol ################################################################

def historicDataComputationForGivenSymbol(symbol_name):

	insert_table_name = 'stock_macd'
	select_table_name = 'sec_bhavdata_full'
	symbol_name = symbol_name
	
	try:

		#print(symbol_name)
		df_select = selectSymbolFunc(select_table_name, symbol_name)
		col_names =  ['SYMBOL', 'TimeStamp', 'CLOSEPRICE', '5DayEMA', '9DayEMA', '12DayEMA', '26DayEMA', '35DayEMA', 'std_macd', 'fast_macd', 'std_macd_sampling', 'fast_macd_sampling', 'UpMomentum', 'DownMomentum']
		df_compute  = pd.DataFrame(columns = col_names)
		if df_select['SYMBOL'].count() > 0:
			dataComputation(df_select, df_compute) ## Call dataComputation function
		else:
			logging.error("No data found in function historicDataComputationForGivenSymbol of MACD_Computation script on data retrived from table: " + select_table_name)
		
		
		if df_compute.empty == False:
			insertFunc(df_compute, insert_table_name) ## Insert computed data into DB
			#print(df_compute)
		else:
			logging.error("The macd value cound not be computed in dataComputation function of MACD_Computation script on data retrived from table: " + select_table_name)
	
	except AttributeError as e:
		logging.error("Error in MACD_Computation script dataComputation function on data retrived from table: " + select_table_name)
		logging.critical(e)


############### The below function is to calculate upward and downward momentum in the prices ####################################

def stockMomentumCalc(df_compute_daily, df_select):
	df_compute_filter_positive = df_compute_daily.loc[df_compute_daily['fast_macd'] >= 0]
	positive_filter_list = df_compute_filter_positive['SYMBOL'].values.tolist()
	df_compute_filter_negative = df_compute_daily.loc[df_compute_daily['fast_macd'] <= 0]
	negative_filter_list = df_compute_filter_negative['SYMBOL'].values.tolist()


	for i in positive_filter_list:
		df_symbol_positive_macd = df_select.loc[df_select['SYMBOL'] == i]
		df_symbol_positive_macd['TimeStamp'] = pd.to_datetime(df_symbol_positive_macd['TimeStamp'])
		last_5_price = df_symbol_positive_macd.sort_values(by = 'TimeStamp').tail(5)['CLOSEPRICE'].values.tolist()
		if last_5_price == sorted(last_5_price):
			df_compute_daily['UpMomentum'][df_compute_daily.SYMBOL == i] = 'Yes'
		else:
			df_compute_daily['UpMomentum'][df_compute_daily.SYMBOL == i] = 'No'
		
	for i in negative_filter_list:
		df_symbol_negative_macd = df_select.loc[df_select['SYMBOL'] == i]
		df_symbol_negative_macd['TimeStamp'] = pd.to_datetime(df_symbol_negative_macd['TimeStamp'])
		last_5_price = df_symbol_negative_macd.sort_values(by = 'TimeStamp').tail(5)['CLOSEPRICE'].values.tolist()
		if last_5_price == sorted(last_5_price, reverse=True):
			df_compute_daily['DownMomentum'][df_compute_daily.SYMBOL == i] = 'Yes'
		else:
			df_compute_daily['DownMomentum'][df_compute_daily.SYMBOL == i] = 'No'
	#print(df_compute_daily)
	return df_compute_daily

############################################################################################################

def main():
	if len(sys.argv) == 2 and sys.argv[1] == 'daily':
		dailyDataComputation("sec_bhavdata_full")
		dailyDataComputation("daily_indices_data",['NIFTY','BANKNIFTY'])
	elif len(sys.argv) == 2 and sys.argv[1] == 'historic':
		historicalDataComputation("sec_bhavdata_full")
		historicalDataComputation("daily_indices_data",['NIFTY','BANKNIFTY'])
	elif len(sys.argv) == 2 and sys.argv[1] == 'newsymbol':
		historicDataComputationForGivenSymbol(symbol_name)
	elif len(sys.argv) != 2:
		print("Usage: python MACD_Computation.py <historic|daily|newsymbol>")
		sys.exit (1)

if __name__ == "__main__":
	main()