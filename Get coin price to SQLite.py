from binance import Client
from binance import BinanceSocketManager
import pandas as pd
import asyncio
import sqlalchemy
import time

api_sec = API_SECURITY
api_key = API_KEY

moeda = 'BTCUSDT'

client = Client(api_key,api_sec)
print("Loggado")
bsm = BinanceSocketManager(client)
engine = sqlalchemy.create_engine('sqlite:///BTCUSDTstream.db')

socket = bsm.trade_socket(moeda)

def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:,['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df

async def main(moeda):
    bm = BinanceSocketManager(client)
    socket = bsm.trade_socket(moeda)
    async with socket as tscm:
        while True:
            msg = await tscm.recv()
            frame = createframe(msg)
            frame.to_sql('BTCUSDT', engine, if_exists='append', index = False)
            print(frame)
    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(moeda))




