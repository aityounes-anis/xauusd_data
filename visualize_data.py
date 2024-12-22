from utils import get_data_from_db
import matplotlib.pyplot as plt
import pandas as pd

def plot_ohlc():
    # SQL query to fetch OHLC data
    query = "SELECT date, close FROM xauusd_prices"
    
    # Fetch data
    data = get_data_from_db(query)
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)

    # Plot closing prices
    plt.figure(figsize=(12, 6))
    plt.plot(data['close'], label='Close Price')
    plt.title('XAU/USD Closing Prices')
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    plot_ohlc()
