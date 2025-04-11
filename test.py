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

    symbol = 'AAPL'
    duration = '5'
    bar_size = '1 day'
    symcon = test_ib.create_stock_contract(symbol)

    df = await test_ib.fetch_all_historical_data(
        contract=symcon,
        duration='5',
        bar_size='1 day',  # or 1 min / 1 hour
        use_rth=True)

    df = df.set_index('date')
    print(df.to_markdown())
    print(df.info())
    # df.to_csv(r'C:\q\w64\aapl_recent.csv')
    df.to_csv(f'{symbol}-{duration}-year-1-{bar_size.split()[-1]}.csv')

    test_ib.disconnect()


asyncio.run(test())
