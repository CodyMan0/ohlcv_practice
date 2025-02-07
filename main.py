import ccxt
import pandas as pd
import os

# 사용자 입력값 변수 (0~3까지 가능, 0이면 기존과 동일)
# 원하는 만큼 이동 (0 = 기존과 동일, 1 = 1시간 후, 2 = 2시간 후, 3 = 3시간 후)
user_shift_hour_offset = 1


# 24시간 기준으로 가지고 올 시간 수정하는 함수
def adjust_target_hours(base_hours, user_shift_hour_offset) :
    return [(hour + user_shift_hour_offset) % 24 for hour in base_hours]



user_shift_hour_offset_for_user = {
    0 : 'same',
    1 : 'move_a_hour',
    2 : 'move_two_hours',
    3 : 'move_three_hours'
}

# 📌 거래소 설정 (Binance)
exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "recvWindow": 50000,
                "adjustForTimeDifference": True,
                "warnOnFetchOpenOrdersWithoutSymbol": False
            }
        })

# 📌 심볼 및 타임프레임 설정
symbol = "BTC/USDT"
timeframe = "1h"  # 1시간 봉 데이터를 가져옴
limit = 1000  # 가져올 캔들 수

# 📌 1시간 봉 데이터 가져오기
ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

# 📌 원하는 시작 시간 설정 (5시, 9시, 13시, 17시, 21시, 1시)
base_target_hours = [5, 9, 13, 17, 21, 1]
target_hours = adjust_target_hours(base_target_hours, user_shift_hour_offset)
print(f"기준 시간 : {target_hours}")

df["hour"] = df["timestamp"].dt.hour  # 시간(hour) 추출
df_filtered = df[df["hour"].isin(target_hours)]  # 원하는 시간대만 필터링

# 📌 4시간 단위로 그룹화
grouped_data = []
for i in range(len(df_filtered) - 1):
    start_row = df_filtered.iloc[i]
    end_row = df_filtered.iloc[i + 1]

    grouped_data.append({
        "timestamp": start_row["timestamp"],  # 첫 번째 데이터의 시간
        "open": start_row["open"],
        "high": max(df.loc[(df["timestamp"] >= start_row["timestamp"]) &
                           (df["timestamp"] < end_row["timestamp"]), "high"]),
        "low": min(df.loc[(df["timestamp"] >= start_row["timestamp"]) &
                          (df["timestamp"] < end_row["timestamp"]), "low"]),
        "close": df.loc[df["timestamp"] < end_row["timestamp"], "close"].iloc[-1],
        "volume": df.loc[(df["timestamp"] >= start_row["timestamp"]) &
                         (df["timestamp"] < end_row["timestamp"]), "volume"].sum()
    })

# 📌 데이터프레임 변환
df_grouped = pd.DataFrame(grouped_data)

# 📌 CSV 파일 저장
output_filename = f"filtered_4h_data_shift_{user_shift_hour_offset_for_user[user_shift_hour_offset]}.csv"
df_grouped.to_csv(output_filename, index=False, encoding="utf-8-sig")

print(f"✅ 4시간 봉 데이터가 {output_filename} 파일로 저장되었습니다.")