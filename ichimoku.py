from historic_OHLC import symbolP, db_path
import sqlite3
import pandas as pd
from inspect import currentframe

# print("symbol:")
# print(symbolP)
# print("db_path:")
# print(db_path)

################################################## Function to read OHLC data from SQLite database #################################################
def read_ohlc_from_db(symbol, db_path):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {symbol}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Convert the 'dateTime' column to datetime format and set it as the index
    df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
    df.set_index('dateTime', inplace=True)

    # Ensure numeric data types
    df[['openPrice', 'highPrice', 'lowPrice', 'closePrice', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span']] = df[['openPrice', 'highPrice', 'lowPrice', 'closePrice', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span']].apply(pd.to_numeric)
    
    return df

################################################## Function to calculate Kumo status #################################################
def calculate_kumo_status(df):
    # Calculate Kumo thickness (difference between Senkou Span A and B)
    df['kumo_thickness'] = abs(df['senkou_span_a'] - df['senkou_span_b'])
    
    # Determine the angle of Senkou Span A and B (basic approach: calculate difference over time)
    df['senkou_span_a_slope'] = df['senkou_span_a'].diff()
    df['senkou_span_b_slope'] = df['senkou_span_b'].diff()

    def determine_kumo_status(row):
        # Kumo thickness threshold for non-deterministic (arbitrary, adjust as needed)
        thickness_threshold = 0.001
        
        if row['kumo_thickness'] < thickness_threshold:
            return 'Non-Deterministic'
        
        # Determine the status based on relative positions and slopes
        if row['senkou_span_a'] > row['senkou_span_b'] and row['senkou_span_a_slope'] > 0:
            return 'Up'
        elif row['senkou_span_a'] < row['senkou_span_b'] and row['senkou_span_a_slope'] < 0:
            return 'Down'
        else:
            return 'Non-Deterministic'
    
    # Apply the function to the dataframe
    df['kumo_status'] = df.apply(determine_kumo_status, axis=1)

    # Print last few rows with Kumo Status
    print(df[['senkou_span_a', 'senkou_span_b', 'kumo_thickness', 'senkou_span_a_slope', 'senkou_span_b_slope', 'kumo_status']].tail(10))
    
    return df

def main():
    df = read_ohlc_from_db(symbolP, db_path)
    df = calculate_kumo_status(df)

# Run the script
if __name__ == "__main__":
    main()