import sqlite3
import pandas as pd
import mplfinance as mpf

db_path = 'ohlc.db'

# Connect to the database
conn = sqlite3.connect(db_path)

# Create a cursor
cursor = conn.cursor()

# Specify the table name for which you want to retrieve column names
table_name = 'ETHUSD'

def datCnv(src):
    return pd.to_datetime(src)

def read_ohlc_from_db(symbol, db_path):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {symbol}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Convert the 'dateTime' column to datetime format and set it as the index
    # df['dateTime'] = pd.to_datetime(df['dateTime'])
    df['dateTime'] = df.dateTime.apply(datCnv)
    df.set_index('dateTime', inplace=True)

    # Ensure numeric data types
    df[['Open', 'High', 'Low', 'Close', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span']] = df[['openPrice', 'highPrice', 'lowPrice', 'closePrice', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span']].apply(pd.to_numeric)
    
    return df

# Function to plot the candlestick chart with Ichimoku Cloud
def plot_candlestick_with_ichimoku(df):
    # Ichimoku Cloud components as additional plots
    ichimoku_lines = [
        mpf.make_addplot(df['tenkan_sen'], color='blue', width=0.75),
        mpf.make_addplot(df['kijun_sen'], color='red', width=0.75),
        mpf.make_addplot(df['senkou_span_a'], color='green', width=0.5),
        mpf.make_addplot(df['senkou_span_b'], color='brown', width=0.5),
        mpf.make_addplot(df['chikou_span'], color='purple', width=0.75)
    ]

    fill_up = dict(y1 = df['senkou_span_a'].values, y2 = df['senkou_span_b'].values, where = df['senkou_span_a'] >= df['senkou_span_b'], alpha = 0.5, color = 'honeydew')
    fill_down = dict(y1 = df['senkou_span_a'].values, y2 = df['senkou_span_b'].values, where = df['senkou_span_a'] < df['senkou_span_b'], alpha = 0.5, color = 'mistyrose')

    # Plot the candlestick chart
    mpf.plot(df, type='candle', style='charles', addplot=ichimoku_lines,
             title=f'{table_name} Ichimoku Cloud',
             volume=False,  # Disable volume for this plot
             fill_between = [fill_up, fill_down]
            #  figratio=(16,9),
            #  figscale=1.5
            )


df = read_ohlc_from_db(table_name, db_path)
plot_candlestick_with_ichimoku(df)

# # Execute the query to fetch the table's schema
# cursor.execute(f"PRAGMA table_info({table_name})")

# # Fetch all rows from the result set
# columns_info = cursor.fetchall()

# # Extract column names from the result set
# column_names = [column_info[1] for column_info in columns_info]
# print(column_names)

# # Execute a SELECT query
# cursor.execute(f"SELECT * FROM {table_name} limit 10")

# # Fetch one row of data
# row = cursor.fetchone()
# print(row)

# fetch all rows of data
# all_rows = cursor.fetchall()
# print(cursor.fetchall())

# # Execute a SELECT query
# cursor.execute(f"SELECT min(startTime), max(startTime), count(*) FROM {table_name} ")
# print(cursor.fetchall())

# Close the cursor and connection
cursor.close()
conn.close()

