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
def get_ticker_df(
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


# ==========================
# 3) 여러 티커 → 하나의 DataFrame
# ==========================
def get_multi_ticker_df(
    tickers: List[str],
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
    features: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    여러 ticker 데이터를 하나의 DataFrame으로 반환.
    컬럼에 ticker 포함, 멀티인덱스(date, ticker) 형태.
    """
    dfs = []
    for t in tickers:
        df_t = get_ticker_df(t, start=start, end=end, features=features)
        if df_t.empty:
            continue
        df_t["ticker"] = t
        dfs.append(df_t)

    if not dfs:
        return pd.DataFrame()

    big = pd.concat(dfs)
    big = big.reset_index().set_index(["date", "ticker"]).sort_index()
    return big


# ==========================
# 4) 딥러닝용 X, y 뽑는 함수 (예시)
# ==========================
def make_supervised_dataset(
    ticker: str,
    window: int = 30,
    pred_horizon: int = 1,
    feature_cols: Optional[List[str]] = None,
    target_col: str = "close",
) -> Tuple[np.ndarray, np.ndarray]:
    """
    단일 ticker에 대해 시계열 윈도우 데이터를 생성.
    X: (N, window, feature_dim)
    y: (N,) or (N, pred_horizon)
    """
    if feature_cols is None:
        feature_cols = ["open", "high", "low", "close", "volume"]

    df = get_ticker_df(ticker)
    df = df[feature_cols + [target_col]].dropna()

    values = df[feature_cols].values
    target = df[target_col].values

    X_list = []
    y_list = []
    for i in range(len(df) - window - pred_horizon + 1):
        X_list.append(values[i : i + window])
        y_list.append(target[i + window + pred_horizon - 1])

    X = np.stack(X_list)  # (N, window, feature_dim)
    y = np.array(y_list)  # (N,)

    return X, y
