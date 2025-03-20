import sqlite3
import yfinance as yf
import pandas as pd
from pathlib import Path
import time

# Gather data from API
ticker = yf.Ticker("MSFT")
data = ticker.history("max")

# Replacing spaces in column names with underscores
data.columns = data.columns.str.replace(" ", "_")

# Define data folder path
data_folder = Path(__file__).parent / 'data'

# Make the dataframe 9.8 million rows
large_df = pd.concat([data] * 1000)

# Define connection and cursor
conn = sqlite3.connect(data_folder / 'market_data.db')
cursor = conn.cursor()

# Create table in SQLite if it doesn't exist
create_sql = "CREATE TABLE IF NOT EXISTS historical_prices " \
"(OPEN FLOAT, " \
"HIGH FLOAT, " \
"LOW FLOAT, " \
"CLOSE FLOAT, " \
"VOLUME INT, " \
"DIVIDENDS FLOAT, " \
"STOCK_SPLITS FLOAT)"

cursor.execute(create_sql)

start_time = time.time()
# Iterate through each row and insert into the table
for row in large_df.itertuples():
    insert_sql = f"INSERT INTO historical_prices (OPEN, HIGH, LOW, CLOSE, VOLUME, DIVIDENDS, STOCK_SPLITS) VALUES ({row.Open}, {row.High}, {row.Low}, {row.Close}, {row.Volume}, {row.Dividends}, {row.Stock_Splits} )"
    cursor.execute(insert_sql)

process_time = time.time() - start_time
print(f"Data wrote to SQLite in {process_time} seconds")

conn.commit()
conn.close()