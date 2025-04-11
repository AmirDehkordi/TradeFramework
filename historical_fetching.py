from data_retriever import IBDataRetriever
from config import IB_HOST, IB_PORT, IB_CLIENT_ID
import logging
import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.getLogger()


async def test():
    test_ib = IBDataRetriever(
        host=IB_HOST,
        port=IB_PORT,
        client_id=IB_CLIENT_ID)

    await test_ib.connect()

    top_50_spx = test_ib.spx_top_k_tickers_marketcap(k=50)
    for symbol in top_50_spx:
        duration = '5' # in years
        bar_size = '1 min'  # or 1 min / 1 hour
        symcon = test_ib.create_stock_contract(symbol)

        df = await test_ib.fetch_all_historical_data(
            contract=symcon,
            duration=duration,
            bar_size=bar_size,
            use_rth=True)

        df = df.set_index('date')
        print(df.to_markdown())
        print(df.info())

        df.to_csv(fr'C:\q\w64\IB_Data_SPX50\{symbol}-{duration}-year-1-{bar_size.split()[-1]}.csv')

    test_ib.disconnect()


asyncio.run(test())
