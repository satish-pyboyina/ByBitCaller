import config
import logging
import pprint
from pybit.unified_trading import HTTP

######################
# get Account Balances
# based on: https://bybit-exchange.github.io/docs/v5/account/wallet-balance
#######################

# establish connection 
api_key=config.API_KEY # expects api key to be present in config.py file
secret_key=config.API_SECRET # expects api secret key to be present in config.py file

logging.basicConfig(filename="pybit.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

# Set testnet=True to use testnet and False to use mainnet
session = HTTP(
    testnet = False,
    api_key = api_key,
    api_secret = secret_key,
)

# preset parameters
accountTypeP = 'CONTRACT'    # Unified account: UNIFIED (trade spot/linear/options), CONTRACT(trade inverse). Normal account: CONTRACT, SPOT
coinP = 'ETH'             # If not passed, it returns non-zero asset info. Pass multiple coins to query, separated by comma. USDT,USDC

def getWalletBalance(accountTypeP,coinP):
    wallet_balance = session.get_wallet_balance(
        accountType = accountTypeP,
        # coin=coinP,                 # If not passed, it returns non-zero asset info. Pass multiple coins to query, separated by comma. USDT,USDC
    )
    return wallet_balance

walletBalance = getWalletBalance(accountTypeP,coinP)

coinBalance = walletBalance['result']['list']

pprint.pprint(walletBalance)

for key,values in coinBalance:
    for i in values:
        for key,values in i:
            if key == 'coin':
                c = values
                for key,values in c:
                    pprint.pprint(f"{key} : {values}")

