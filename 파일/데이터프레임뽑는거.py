from data_loader import get_ticker_df, make_supervised_dataset

# 1) DataFrame으로만 쓸 때
df_aapl = get_ticker_df("AAPL")
print(df_aapl.head())

# 2) 딥러닝용 입력 만들 때 (예: 30일로 1일 뒤 close 예측)
X, y = make_supervised_dataset("AAPL", window=30, pred_horizon=1)

print(X.shape)  # (N, 30, feature_dim)
print(y.shape)  # (N,)
