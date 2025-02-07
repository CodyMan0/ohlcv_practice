import ccxt
import pandas_example as pd
import os

# 📌 거래소 설정 (Binance)
exchange = ccxt.binance()

# 📌 심볼 및 타임프레임 설정
symbol = "BTC/USDT"
timeframe = "1h"  # 1시간 봉 데이터를 가져옴
limit = 1000  # 가져올 캔들 수

# 📌 1시간 봉 데이터 가져오기
ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

# 📌 원하는 시간대 필터링 (5시, 9시, 13시, 15시, 19시)
hours = [5, 9, 13, 17, 21, 1]
df["hour"] = df["timestamp"].dt.hour  # 시(hour) 추출
df_filtered = df[df["hour"].isin(hours)]

# 📌 4시간 봉으로 변환 (OHLCV 계산)
df_grouped = df_filtered.resample("4h", on="timestamp").agg({
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum"
}).dropna().reset_index()

# 📌 CSV 파일 저장
output_filename = "filtered_4h_data.csv"
df_grouped.to_csv(output_filename, index=False, encoding="utf-8-sig")

print(f"✅ 4시간 봉 데이터가 {output_filename} 파일로 저장되었습니다.")

