import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
import os
import matplotlib.pyplot as plt

# Load environment variables from .env file
load_dotenv()

# MySQL connection details from .env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Fetch data from MySQL
def fetch_data_from_mysql():
    connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(connection_string)
    query = "SELECT * FROM xauusd_data WHERE date > '2020-01-01' ORDER BY date;"
    df = pd.read_sql(query, con=engine)

    # Format the DataFrame
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df

# Refine Z-Score strategy
def refine_z_score_strategy(df):
    # Volatility threshold (e.g., top 25% of historical volatility)
    vol_threshold = df['volatility_20d'].quantile(0.75)

    # Refined signals: Only act on Z-Score signals when volatility is high
    df['refined_signal'] = 0
    df.loc[(df['z_score'] < -2) & (df['volatility_20d'] > vol_threshold), 'refined_signal'] = 1  # Buy
    df.loc[(df['z_score'] > 2) & (df['volatility_20d'] > vol_threshold), 'refined_signal'] = -1  # Sell

    # Calculate strategy returns
    df['refined_strategy_return'] = df['refined_signal'].shift(1) * df['log_return']
    df['refined_cumulative_return'] = (1 + df['refined_strategy_return']).cumprod()

    return df

# Backtest and evaluate the strategy
def evaluate_strategy(df):
    # Performance metrics
    sharpe_ratio = df['refined_strategy_return'].mean() / df['refined_strategy_return'].std() * np.sqrt(252)  # Annualized Sharpe
    max_drawdown = (df['refined_cumulative_return'] / df['refined_cumulative_return'].cummax() - 1).min()

    print("Performance Metrics:")
    print(f"Sharpe Ratio: {sharpe_ratio:.2f}")
    print(f"Maximum Drawdown: {max_drawdown:.2%}")

# Visualize cumulative returns
def plot_cumulative_returns(df):
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['refined_cumulative_return'], label='Refined Z-Score Strategy', color='blue')
    plt.title('Cumulative Return of Refined Z-Score Strategy')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return')
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

# Main execution
if __name__ == "__main__":
    print("Fetching data from MySQL...")
    df = fetch_data_from_mysql()

    print("Refining strategy...")
    df = refine_z_score_strategy(df)

    print("Evaluating strategy...")
    evaluate_strategy(df)

    print("Visualizing cumulative returns...")
    plot_cumulative_returns(df)