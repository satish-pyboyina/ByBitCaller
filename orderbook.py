from pybit.unified_trading import HTTP
import json

######################
# Pulls Order Book data
# based on: https://bybit-exchange.github.io/docs/v5/market/orderbook
#######################

# API endpoint URL
# url = 'https://api.bybit.com' #mainnet
# url = 'https://api-testnet.bybit.com' #testnet
# print(url)

# testnet=True means your API keys were generated on testnet.bybit.com
session = HTTP(testnet=False)

# set parameters
categoryP = 'inverse'   # Product type. Allowed values: spot, linear, inverse, option
symbolP = 'ETHUSD'      # e.g. For linear use 'ETHUSDT'; for inverse use 'ETHUSD'
limitP = 200           # Limit size for each bid and ask. spot: [1, 50]. Default: 1. linear&inverse: [1, 200]. Default: 25. option: [1, 25]. Default: 1.

if categoryP == 'spot' and limitP > 50:
    limitP = 50

if (categoryP == 'linear' or categoryP == 'inverse') and limitP > 200:
    limitP = 200

if limitP > 25:
    limitP = 25

orderbook=(session.get_orderbook(
    category = categoryP,
    symbol = symbolP,
    limit = limitP,
))

filename = f'{symbolP}-orderbook.json'
with open(filename, 'a') as file:
    json.dump(orderbook, file)