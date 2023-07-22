import config, json
import requests
import time
import hashlib
import hmac
import uuid
from pybit.unified_trading import HTTP

# https://github.com/bybit-exchange/api-usage-examples/blob/master/V5_demo/api_demo/Encryption_HMAC.py

api_key=config.API_KEY
secret_key=config.API_SECRET
httpClient=requests.Session()
recv_window=str(5000)

# API endpoint URL
url = 'https://api.bybit.com'
print(url)

session = HTTP(testnet=False)

def HTTP_Request(endPoint,method,payload,Info):
    global time_stamp
    time_stamp=str(int(time.time() * 10 ** 3))
    signature=genSignature(payload)
    headers = {
        'X-BAPI-API-KEY': api_key,
        'X-BAPI-SIGN': signature,
        'X-BAPI-SIGN-TYPE': '2',
        'X-BAPI-TIMESTAMP': time_stamp,
        'X-BAPI-RECV-WINDOW': recv_window,
        'Content-Type': 'application/json'
    }
    if(method=="POST"):
        response = httpClient.request(method, url+endPoint, headers=headers, data=payload)
    else:
        response = httpClient.request(method, url+endPoint+"?"+payload, headers=headers)
    print(response.text)
    print(Info + " Elapsed Time : " + str(response.elapsed))

def genSignature(payload):
    param_str= str(time_stamp) + api_key + recv_window + payload
    hash = hmac.new(bytes(secret_key, "utf-8"), param_str.encode("utf-8"),hashlib.sha256)
    signature = hash.hexdigest()
    return signature


ohlc=(session.get_kline(
    category="inverse",
    symbol="ETHUSD",
    interval=5,
    start=1672491600000,
    limit=4,
))

# print(session.get_tickers(
#     category="inverse",
#     symbol="ETHUSD",
# ))

# response=session.get_tickers(category="inverse",symbol="ETHUSD",)

# data = response.json()  # Parse the JSON response

# print(response)

# with open('api_response.json', 'w') as file:
#     file.write(response.text)

# if response.status_code == 200:
    # data = response.json()  # Parse the JSON response
    # access_token = data['access_token']  # Assuming the API returns an access token

# Write the API response to a file
with open('ETH-2023-ohlc.json', 'w') as file:
    json.dump(ohlc, file)
print("API response has been saved to 'ETH-2023-ohlc.json'")
