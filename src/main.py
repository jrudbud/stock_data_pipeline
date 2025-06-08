import yfinance as yf
import pandas as pd
from datetime import datetime
import os

def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch stock data from yfinance."""
    data = yf.download(ticker, start=start_date, end=end_date)
    if data is None:
        return pd.DataFrame()
    return data

def save_to_csv(data: pd.DataFrame, ticker: str, prefix: str = "latest") -> str:
    """Save data to CSV with timestamp or prefix."""
    filename = f"{ticker}_{prefix}_{datetime.now().strftime('%Y%m%d')}.csv"
    data.to_csv(filename)
    return filename

if __name__ == "__main__":
    # Example: Fetch AAPL data
    ticker = "AAPL"
    data = fetch_stock_data(ticker, start_date="2023-01-01", end_date="2023-12-31")
    
    # Save to "latest" and "historical" (if needed)
    latest_file = save_to_csv(data, ticker, prefix="latest")
    print(f"Data saved to {latest_file}")