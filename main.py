## exists Private && Public



import datetime
import json
import time
from time import sleep
from turtledemo.penrose import start

import ccxt.pro as ccxtpro
import asyncio
import pandas as pd

# 교환소 설정


#
# 데이터 설정
# symbol = 'BTC/USDT'  # 비트코인 대 USDT
# timeframe = '1d'     # 일별 데이터
# since = exchange.parse8601('1 year ago UTC')  # 1년 전부터 현재까지
#
# # 데이터 수집
symbols = list()

def get_top_20_by_volume_1:
    pass

def get_top_20_by_volume_1:
    pass


async def test() :
    exchange = ccxtpro.binance({
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "recvWindow": 50000,
            "adjustForTimeDifference": True,
            "warnOnFetchOpenOrdersWithoutSymbol": False
        }
    })
    ohlcv = await exchange.fetch_tickers()
    with open("data.txt", "w") as file:
        file.write(json.dumps(ohlcv))
    ticker_detials = json.dumps(ohlcv)
    print(len(ticker_detials))
    for symbol, ticker_detail in ohlcv.items():
        # if symbol.split(":")[-1].lower() == "usdt":
        #     continue
        # if "USDT" in symbol :
        #     continue
        # print(ticker_detail)
        symbols.append(symbol)
        for symbol in symbols:
            print(symbol)
        print(len(symbols))

    await exchange.close()

    start_time = time.time()

    finish_time = time.time()
    print(f"elapsed: {finish_time - start_time}")
    return


# # Pandas 데이터프레임으로 변환
# df = pd.DataFrame(ohlcv, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
# df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
# df.set_index('Date', inplace=True)
# del df['Timestamp']
#
# # CSV 파일로 저장
# df.to_csv("bitcoin_ohlcv.csv")

# 내장된 객체?
if __name__ == "__main__":
    asyncio.run(test())
    print('hello')