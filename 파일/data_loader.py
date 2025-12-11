# data_loader.py
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from typing import List, Optional, Tuple
import numpy as np

# ==========================
# 1) MongoDB 연결 함수
# ==========================
def get_mongo_collection(
    uri: str = "mongodb+srv://aaaa:aaaa@cluster0.tynsmmq.mongodb.net/?appName=Cluster0",
    db_name: str = "stock_db",
    col_name: str = "daily_prices",
):
    client = MongoClient(uri, server_api=ServerApi("1"))
    db = client[db_name]
    col = db[col_name]
    return col


col = get_mongo_collection()


# ==========================
# 2) 단일 티커 → DataFrame
# ==========================
def ticker(
    ticker: str,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
    features: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    특정 ticker의 일봉 데이터를 DataFrame으로 반환.
    date 인덱스, 오름차순 정렬.
    features가 None이면 [open, high, low, close, volume, ticker] 전부.
    """

    query: dict = {"ticker": ticker}
    if start or end:
        date_cond = {}
        if start:
            date_cond["$gte"] = pd.to_datetime(start)
        if end:
            date_cond["$lte"] = pd.to_datetime(end)
        query["date"] = date_cond

    projection = {"_id": 0}
    cursor = col.find(query, projection)
    data = list(cursor)

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")

    if features is not None:
        keep = [f for f in features if f in df.columns]
        df = df[keep]

    return df

def ticker_list():
    tickers = ["AAPL", "ABBV", "ABT", "ACN", "AIG", "AMGN", "AMT", "AMZN", "AVGO", "AXP",
    "BA", "BAC", "BK", "BKNG", "BLK", "BMY", "BRK-B", "C", "CAT", "CHTR",
    "CL", "CMCSA", "COF", "COP", "COST", "CRM", "CSCO", "CVS", "CVX", "DD",
    "DE", "DHR", "DIS", "DOW", "DUK", "EMR", "EXC", "F", "FDX", "FOX",
    "FOXA", "GD", "GE", "GILD", "GM", "GOOG", "GOOGL", "GS", "HD", "HON",
    "IBM", "INTC", "INTU", "JNJ", "JPM", "KHC", "KMI", "KO", "LIN", "LLY",
    "LMT", "LOW", "MA", "MCD", "MDLZ", "MDT", "MET", "META", "MMM", "MO",
    "MRK", "MS", "MSFT", "NEE", "NFLX", "NKE", "NVDA", "ORCL", "PEP", "PFE",
    "PG", "PM", "PYPL", "QCOM", "RTX", "SBUX", "SO", "SPG", "T", "TGT",
    "TMO", "TXN", "UNH", "UNP", "UPS", "USB", "V", "VZ", "WFC", "WMT", "XOM"]
    return tickers


