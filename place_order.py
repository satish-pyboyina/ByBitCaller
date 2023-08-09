import config
# import requests
import logging
from pybit.unified_trading import HTTP

######################
# Put a buy or sell order
# based on: https://bybit-exchange.github.io/docs/v5/order/create-order
#######################

# establish connection 
api_key=config.API_KEY # expects api key to be present in config.py file
secret_key=config.API_SECRET # expects api secret key to be present in config.py file
# httpClient=requests.Session()
# recv_window=str(5000)

logging.basicConfig(filename="pybit.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

# API endpoint URL
# url = 'https://api.bybit.com' #mainnet
# url = 'https://api-testnet.bybit.com' #testnet
# print(url)

# Set testnet=True to use testnet and False to use mainnet
session = HTTP(
    testnet = False,
    api_key = api_key,
    api_secret = secret_key,
)

# preset parameters
categoryP = 'inverse'   # Product type. Allowed values: spot, linear, inverse, option
symbolP = 'ETHUSD'      # e.g. For linear use 'ETHUSDT'; for inverse use 'ETHUSD'
orderTypeP = 'Market'   #Allowed values: Market, Limit
timeInForceP = 'IOC' # If not passed, GTC is used by default. Market order will use IOC directly.
orderLinkIdP = f'{symbolP}-{categoryP}-{orderTypeP}-{timeInForceP}'


# defaults for dynamic parameters
sideP = 'Buy'
qtyP = '0.1'
# priceP = "15600" # not applicable for market orders
isLeverageP = 0
positionIdxP = 1 # Under hedge-mode, this param is required. 0: one-way mode, 1: hedge-mode Buy side, 2: hedge-mode Sell side



print(session.place_order(
    category = categoryP,
    symbol = symbolP,
    side = sideP,
    orderType = orderTypeP,
    qty = qtyP,
    # price = "15600", # not applicable for market orders
    # timeInForce = timeInForceP, # Market order will use IOC directly.
    orderLinkId = orderLinkIdP,
    isLeverage = isLeverageP,
    # orderFilter = "Order", # valid for spot only

))