import config, json
import requests
import time
import hashlib
import hmac
import uuid
from pybit.unified_trading import HTTP
import datetime as dt
import pandas as pd
import sqlite3
from time import sleep
from inspect import currentframe


# https://github.com/bybit-exchange/api-usage-examples/blob/master/V5_demo/api_demo/Encryption_HMAC.py

api_key=config.API_KEY
secret_key=config.API_SECRET
httpClient=requests.Session()
recv_window=str(5000)

# API endpoint URL
url = 'https://api.bybit.com'
# print(url)

session = HTTP(testnet=False)

# def HTTP_Request(endPoint,method,payload,Info):
#     global time_stamp
#     time_stamp=str(int(time.time() * 10 ** 3))
#     signature=genSignature(payload)
#     headers = {
#         'X-BAPI-API-KEY': api_key,
#         'X-BAPI-SIGN': signature,
#         'X-BAPI-SIGN-TYPE': '2',
#         'X-BAPI-TIMESTAMP': time_stamp,
#         'X-BAPI-RECV-WINDOW': recv_window,
#         'Content-Type': 'application/json'
#     }
#     if(method=="POST"):
#         response = httpClient.request(method, url+endPoint, headers=headers, data=payload)
#     else:
#         response = httpClient.request(method, url+endPoint+"?"+payload, headers=headers)
#     print(response.text)
#     print(Info + " Elapsed Time : " + str(response.elapsed))

# def genSignature(payload):
#     param_str= str(time_stamp) + api_key + recv_window + payload
#     hash = hmac.new(bytes(secret_key, "utf-8"), param_str.encode("utf-8"),hashlib.sha256)
#     signature = hash.hexdigest()
#     return signature

categoryP = 'inverse'
symbolP = 'ETHUSD'
intervalP = 10
limitP = 1000

year = 2023
month = 1
day = 1

# startDt = dt.datetime(year, month, day)
# endDt = dt.datetime.now()

# startTime = str(int(startDt.timestamp())*1000)
# endTime   = str(int(endDt.timestamp())*1000)

def get_linenumber():
    cf = currentframe()
    global line_number
    line_number = cf.f_back.f_lineno

def get_bybit_bars(categoryP, symbolP, intervalP, startTime, endTime, limitP):

    # url = 'https://api.bybit.com/public/linear/kline'
    print("Start: " + str(startTime) + " , End: " + str(endTime))
    startTime = str(int(startTime.timestamp())*1000)
    endTime   = str(int(endTime.timestamp())*1000)
    print("StartTime: " + startTime + " , EndTime: " + endTime)

    ohlc=(session.get_kline(
        category=categoryP,
        symbol=symbolP,
        interval=intervalP,
        start=startTime, # 1675170000000, # 1 Feb 2023
        end=endTime, #1690293600000, # 26 July 2023
        limit=limitP,
    ))

    with open(f'ETH-2023-ohlc.json', 'w') as file:
        json.dump(ohlc, file)
    print(f"API response has been saved to 'ETH-2023-ohlc.json'")

    # req_params = {'category' : category, 'symbol' : symbol, 'interval' : interval, 'from' : startTime, 'to' : endTime}
    # df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text)['result'])

    data_list = ohlc['result']['list']
    extracted_data = []

    for item in data_list:
        dateTime = int(item[0])/1000
        startTime = item[0]
        openPrice = item[1]
        highPrice = item[2]
        lowPrice = item[3]
        closePrice = item[4]

        extracted_data.append([dateTime,startTime, openPrice, highPrice, lowPrice, closePrice])

    # df = pd.DataFrame(ohlc)
    df = pd.DataFrame(extracted_data, columns=['dateTime','startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice'])

    top_10_rows = df.head(10)
    print(top_10_rows)

    if (len(df.index) == 0):
        return None
    
    # df.index = [dt.datetime.fromtimestamp(x) for x in df.open_time]
    df.index = [dt.datetime.fromtimestamp(x) for x in df.dateTime]
    print('index')
    #convert time in seconds to timestamp
    df['dateTime'] = pd.to_datetime(df['dateTime']*1000, errors='coerce')
    return df

# print("StartDt: " + str(startDt) + " , EndDt: " + str(endDt) + " , StartTime: " + startTime + " , EndTime: " + endTime)



# print(session.get_tickers(
#     category="inverse",
#     symbol="ETHUSD",
# ))

# response=session.get_tickers(category="inverse",symbol="ETHUSD",)

# data = response.json()  # Parse the JSON response

# print(response)

# Write the API response to a file
# with open('ETH-2023-ohlc.json', 'w') as file:
#     json.dump(ohlc, file)
# print("API response has been saved to 'ETH-2023-ohlc.json'")

df_list = []
last_datetime = dt.datetime(year, month, day)

while True:

    print(last_datetime)
    new_df = get_bybit_bars(categoryP, symbolP, intervalP, last_datetime, dt.datetime.now(),limitP)
    if new_df is None:
        break
    df_list.append(new_df)
    last_datetime = max(new_df.index) + dt.timedelta(0, 1)
    df = pd.concat(df_list)

    # print(df)
    # df.to_parquet('ETH-2023-ohlc.parquet')

    try:               

        conn = sqlite3.connect('ohcl.db')
        column_names=['time','startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice']
        df.to_sql(f'{symbolP}', conn, if_exists='replace', index=False)

        conn.commit()
        conn.close()
            
    except Exception as e:
        get_linenumber()
        print(line_number, 'exeception: {}'.format(e))


    sleep(0.1)