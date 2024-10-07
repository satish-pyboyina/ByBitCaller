import config, json
import requests
# import time
# import hashlib
# import hmac
# import uuid
from pybit.unified_trading import HTTP
import datetime as dt
import pandas as pd
import sqlite3
from time import sleep
from inspect import currentframe

#   Pulls historic OHLC data from ByBit for given Symbol from given date at specified interval
#   Writes output to a JSON file and SQL database

# Based of following scripts:
#   https://github.com/bybit-exchange/api-usage-examples/blob/master/V5_demo/api_demo/Encryption_HMAC.py
#   https://gist.github.com/so1tsuda/7de86e94a8b39fc141daddd78990e5fc#file-bybit_get_historical_kline-py
#   https://github.com/ryu878/Bybit-OHLC-Data-Saver/blob/main/get_ohlc.py


#   Based on ByBit API Version 5
#   https://bybit-exchange.github.io/docs/v5/market/index-kline


# API endpoint URL
# url = 'https://api.bybit.com' #mainnet
# url = 'https://api-testnet.bybit.com' #testnet
# print(url)

# testnet=True means your API keys were generated on testnet.bybit.com
session = HTTP(testnet=False)

# set parameters
categoryP = 'inverse'   # Allowed values: linear,inverse
symbolP = 'ETHUSD'      # For linear use 'ETHUSDT'; for inverse use 'ETHUSD'
intervalP = 60 # Time interval for pulling data. Allowed values: 1,3,5,15,30,60,120,240,360,720,D,M,W
limitP = 1000 # ByBit applies 1000 limit
db_path = 'ohlc.db'  # Path to your SQLite database

# restricting limit to avoid user error
if limitP > 1000:
    limitP = 1000

# Set Start Date
year = 2024
month = 1
day = 1


def get_linenumber():
    cf = currentframe()
    global line_number
    line_number = cf.f_back.f_lineno

def get_bybit_bars(categoryP, symbolP, intervalP, startTime, limitP):
    ohlc=(session.get_kline(
        category=categoryP,
        symbol=symbolP,
        interval=intervalP,
        start=startTime, 
        # end=endTime, # if endTime is mentioned data is pulled backwards from endTime onwards. If startTime is lower than (endTime - limit) it will be discarded.
        limit=limitP,
    ))

    # data_list = ohlc['result']['list']
    
    # Check if the expected keys exist in the response
    if 'result' in ohlc and 'list' in ohlc['result']:
        data_list = ohlc['result']['list']
    else:
        print("No data found in the response")
        return None
    
    # print("dl rows:")
    # dlrows=len(data_list)
    # print(dlrows)

    extracted_data = []

    # top_10_rows = data_list[:10]
    # print("Top 10 rows from data_list:")
    # for row in top_10_rows:
    #     print(row)

    for item in data_list:
        if isinstance(item, list) and len(item) >= 5:
            dateTime = int(item[0])/1000
            startTime = item[0]
            openPrice = item[1]
            highPrice = item[2]
            lowPrice = item[3]
            closePrice = item[4]
            if None not in [dateTime, startTime, openPrice, highPrice, lowPrice, closePrice]:
                extracted_data.append([dateTime, startTime, openPrice, highPrice, lowPrice, closePrice])
            else:
                print(f"Invalid data detected: {[dateTime, startTime, openPrice, highPrice, lowPrice, closePrice]}")
        else:
            print(f"Unexpected data format: {item}")

    # rows=len(extracted_data)
    # print(rows)
    # top_10_extracted = extracted_data[:2]
    # print("Top 10 extracted rows:")
    # for row in top_10_extracted:
    #     print(row)

    # Convert to DataFrame
    df = pd.DataFrame(extracted_data, columns=['dateTime', 'startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice'])
    # df = pd.DataFrame(extracted_data, columns=['dateTime','startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice'])

    # capture and print top 10 rows of dataframe
    # top_10_rows = df.head(10)
    # print(top_10_rows)

    # Check if the dataframe is empty
    if df.empty:
        # print("Data Frame is blank!")
        return None
    
    # write output to a file
    # filename = 'ETH-2023-ohlc-' + startTime + '.json'
    filename = f'{symbolP}-{year}-{month}-ohlc.json'
    with open(filename, 'a') as file:
        json.dump(ohlc, file)
    # print(f"API response has been saved to {filename}")
    # print("------------------------------------------")

    df.index = [dt.datetime.fromtimestamp(x) for x in df.dateTime]
    # print('index: ' + str(df.index))
    #convert time in seconds to milliseconds
    df['dateTime'] = pd.to_datetime(df['dateTime']*1000, unit='ms')
    return df


################################################## Calculate Ichimoku Cloud components ##################################################
def add_ichimoku(df):
    nine_period_high = df['highPrice'].rolling(window=9).max()
    nine_period_low = df['lowPrice'].rolling(window=9).min()
    df['tenkan_sen'] = (nine_period_high + nine_period_low) / 2

    period26_high = df['highPrice'].rolling(window=26).max()
    period26_low = df['lowPrice'].rolling(window=26).min()
    df['kijun_sen'] = (period26_high + period26_low) / 2

    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)

    period52_high = df['highPrice'].rolling(window=52).max()
    period52_low = df['lowPrice'].rolling(window=52).min()
    df['senkou_span_b'] = ((period52_high + period52_low) / 2).shift(26)

    df['chikou_span'] = df['closePrice'].shift(-26)

    return df

df_list = []
start_datetime = dt.datetime(year, month, day) 
startTime = str(int(start_datetime.timestamp())*1000) # determine date in milliseconds
endTime   = str(int(startTime) + (limitP*60*1000*intervalP)) # calculated end time in milliseconds
# Though variable is named endTime, data for this last timestamp is not pulled

# print("start_datetime: " + str(start_datetime))
# print("StartTime: " + startTime + " , EndTime: " + endTime) # print in milliseconds format
# print("StartTimeStamp: " + str(dt.datetime.fromtimestamp(int(startTime)/1000)) + " , EndTimeStamp: " + str(dt.datetime.fromtimestamp(int(endTime)/1000))) # print in timestamp format

while True:
    new_df = get_bybit_bars(categoryP, symbolP, intervalP, startTime, limitP)

    # if new_df is None:
    #     print("new_df is empty")
    #     break
    # else:
        # print("new_df got data")
    
    # Add Ichimoku Cloud components
    if new_df is None:
        # print("Finished or Failed retrieving data. Exiting.")
        break
    else:
        new_df = add_ichimoku(new_df)
 
    df_list.append(new_df)
    startTime = endTime # as data is not pulled for endTime timestamp last time, startTime is set to previous loop's endTime
    endTime   = str(int(startTime) + (limitP*60*1000*intervalP))

    # first_datetime = min(new_df.index)
    # last_datetime = max(new_df.index)

    # print("dl df_list:")
    # dlrows=len(df_list)
    # print(dlrows)

    df = pd.concat(df_list)

    print("Total Rows:", len(df.index))

    try:

        conn = sqlite3.connect(db_path)
        
        # Check if DataFrame is empty
        if df.empty:
            print("DataFrame is empty. No data to write to the database.")
            break
        else:
            # Display the DataFrame columns for verification
            # print("DataFrame Columns:", df.columns)

            # Ensure the DataFrame contains all necessary columns
            expected_columns = ['dateTime', 'startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span']
            
            for col in expected_columns:
                if col not in df.columns:
                    print(f"Missing expected column: {col}")

            # Write to database
            df.to_sql(name=symbolP, con=conn, if_exists='replace', index=False)

            # Commit and close the connection
            conn.commit()

            # Count rows added to target table
            # cursor = conn.cursor()
            # cursor.execute(f'select count(*) from {symbolP}')
            # results = cursor.fetchall()
            # print(results)

        conn.close()
            
    except Exception as e:
        get_linenumber()
        print(line_number, 'exeception: {}'.format(e))


    sleep(0.1)