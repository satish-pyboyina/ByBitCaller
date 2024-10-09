import sqlite3
import requests
import pandas as pd
import json
from inspect import currentframe
from historic_OHLC import categoryP, symbolP, limitP, db_path
from datetime import datetime

# API URL
api_url = 'https://api.bybit.com/v5/market/recent-trade'

# Set default aggregation by minutes
agg_by_mins = 5

def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno

def fetch_recent_trades(category, symbol, limit):
    try:
        params = {
            'category': category,
            'symbol': symbol,
            'limit': limit
        }
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            data = response.json()['result']['list']
            return data
        else:
            print(f"Error fetching data: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error at line {get_linenumber()}: {e}")
        return None

def save_raw_data(trades, symbol, category):
    """
    Saves the raw trade data into its own table named {symbol}_{category}_data.
    Appends to the table if it exists but does not insert rows with duplicate 'execId'.
    """
    try:
        conn = sqlite3.connect(db_path)
        table_name = f"{symbol}_{category}_data"
        
        # Convert the trades into a DataFrame
        df = pd.DataFrame(trades)
        
        # Check if table exists and append data if it does
        query = f"SELECT execId FROM {table_name}"
        
        try:
            # Fetch existing execIds from the table
            existing_df = pd.read_sql(query, conn)
            
            # Filter out rows with duplicate execIds
            df = df[~df['execId'].isin(existing_df['execId'])]
        except Exception:
            # Table does not exist, so no filtering is needed
            pass
        
        # Append the remaining rows to the table
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
        
        # Commit and close the connection
        conn.commit()
        conn.close()
        
        print(f"Raw trade data successfully appended to {table_name} table.")
    
    except Exception as e:
        print(f"Database error at line {get_linenumber()}: {e}")

def save_to_database_summary(symbol, category, agg_by_mins):
    """
    Saves the summarized trade data into its own table named {symbol}_{category}_summary.
    Sources the data from the {symbol}_{category}_data table.
    """
    try:
        conn = sqlite3.connect(db_path)
        table_name = f"{symbol}_{category}_summary"
        
        # Source data from the {symbol}_{category}_data table
        data_table = f"{symbol}_{category}_data"
        df = pd.read_sql(f"SELECT * FROM {data_table}", conn)
        
        # Cast 'size' to numeric to avoid concatenation issues
        df['size'] = pd.to_numeric(df['size'], errors='coerce')
        
        # Convert 'time' to datetime format
        df['dateTime'] = pd.to_datetime(pd.to_numeric(df['time'], errors='coerce'), unit='ms')
        df.set_index('dateTime', inplace=True)

        # Separate buy and sell data for size summing before resampling
        df['buy_size'] = df.apply(lambda row: row['size'] if row['side'] == 'Buy' else 0, axis=1)
        df['sell_size'] = df.apply(lambda row: row['size'] if row['side'] == 'Sell' else 0, axis=1)

        # Resample for 5-minute intervals and aggregate data
        df_resampled = df.resample(f'{agg_by_mins}min').agg({
            'price': ['max', 'min'],  # Get max and min price
            'buy_size': 'sum',        # Total Buy size
            'sell_size': 'sum'        # Total Sell size
        })
        
        # Flatten the column structure and rename columns
        df_resampled.columns = ['max_price', 'min_price', 'buy_size', 'sell_size']
        
        # Add start_time and end_time for each interval
        df_resampled['start_time'] = df_resampled.index
        df_resampled['end_time'] = df_resampled.index + pd.Timedelta(minutes=agg_by_mins)
        
        # Reset the index to allow saving to the database
        df_resampled.reset_index(drop=True, inplace=True)
        
        # Save the resulting DataFrame to the SQLite database
        df_resampled.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        
        # Commit and close connection
        conn.commit()
        conn.close()
        
        print(f"Summary trade data successfully saved to {table_name} table.")
    
    except Exception as e:
        print(f"Database error at line {get_linenumber()}: {e}")

if __name__ == "__main__":
    # Fetch recent trades
    recent_trades = fetch_recent_trades(categoryP, symbolP, limitP)
    
    if recent_trades:
        # Save raw data to a new table or append to existing
        save_raw_data(recent_trades, symbolP, categoryP)
        
        # Save summarized data to another table
        save_to_database_summary(symbolP, categoryP, agg_by_mins)
    else:
        print("No data to save.")
