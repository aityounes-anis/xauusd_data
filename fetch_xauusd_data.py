import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import os

# Load environment variables
load_dotenv()

# MySQL connection details
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# Alpha Vantage API configuration
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
SYMBOL = "XAUUSD"

# Fetch data from Alpha Vantage
def fetch_data():
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": SYMBOL,
        "market": "USD",
        "apikey": API_KEY,
        "outputsize": "full"  # Fetch full historical data
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if "Time Series (Daily)" in data:
        return data["Time Series (Daily)"]
    else:
        print("Error fetching data:", data)
        return None

# Calculate indicators
def calculate_indicators(data):
    df = pd.DataFrame.from_dict(data, orient='index', dtype=float)
    df.index = pd.to_datetime(df.index)  # Convert the index to datetime
    df.sort_index(inplace=True)  # Sort data by date

    # Rename columns to match database schema
    df.rename(columns={
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close"
    }, inplace=True)

    # Calculate indicators
    df['Log_Returns'] = np.log(df['Close'] / df['Close'].shift(1))
    df['Volatility_20d'] = df['Log_Returns'].rolling(window=20).std()
    df['Daily_Range'] = df['High'] - df['Low']
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['Z_Score'] = (df['Close'] - df['SMA_20']) / df['Volatility_20d']

    # Drop rows with NaN values (from rolling calculations)
    df.dropna(inplace=True)

    return df

# Store Data with Indicators
def store_data(data):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Ensure database table includes columns for indicators
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS xauusd_data (
            date DATE PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            log_return DOUBLE,
            volatility_20d DOUBLE,
            daily_range DOUBLE,
            z_score DOUBLE
        );
    """)
    conn.commit()

    for date, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO xauusd_data (
                    date, open, high, low, close, log_return, volatility_20d, daily_range, z_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    log_return=VALUES(log_return),
                    volatility_20d=VALUES(volatility_20d),
                    daily_range=VALUES(daily_range),
                    z_score=VALUES(z_score)
            """, (
                date,
                row['Open'], row['High'], row['Low'], row['Close'],
                row['Log_Returns'], row['Volatility_20d'], row['Daily_Range'], row['Z_Score']
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting data for {date}: {e}")

    cursor.close()
    conn.close()

# Main execution
if __name__ == "__main__":
    print("Fetching data from Alpha Vantage...")
    raw_data = fetch_data()
    if raw_data:
        print("Data fetched successfully! Calculating indicators...")
        df_with_indicators = calculate_indicators(raw_data)
        print("Indicators calculated. Storing in database...")
        store_data(df_with_indicators)
        print("Data storage complete!")
    else:
        print("No data to store.")
