# import spacy
# import os
# import re
# import asyncio
# import nest_asyncio
# import pandas as pd
# import time
# from ib_insync import *
# import math
# import datetime
# import concurrent.futures
# # import config as c
#
#
# IB_IP, IB_PT, CL_ID = '127.0.0.1', 4002, 42
# risk = 0.001
# max_margin_allowed = 0.9
# trade_close_time = '15:55'
#
# date_today = datetime.datetime.today().strftime('%Y-%m-%d')
# print('====================')
# print(f'Date: {date_today}')
# print('====================')
#
# print('========== Connecting to IB ==========')
# ib = IB()
# print(f"Connecting to IB at {IB_IP}:{IB_PT} with Client ID={CL_ID}...")
# try:
#     ib.connect(IB_IP, IB_PT, clientId=CL_ID)
# except Exception as e:
#     raise RuntimeError(f"Failed to connect to IB: {e}")
#
# if not ib.isConnected():
#     raise RuntimeError("Connection to IB could not be established.")
# print("Successfully connected to IB.")
# print()
#
#
# contract = Stock('AAPL', 'SMART', 'USD')
# ib.qualifyContracts(contract)
#
# # Real-time market data (streaming)
# ib.reqMktData(contract)
#
# # Historical data
# bars = ib.reqHistoricalData(
#     contract,
#     endDateTime='',
#     durationStr='1 D',
#     barSizeSetting='1 min',
#     whatToShow='TRADES',
#     useRTH=False
# )
#
# # Convert to a pandas DataFrame
# df = util.df(bars)
#
# print(df.info())
# print(df.head())  # Print the first few rows
# print(df.tail())  # Print the last few rows
#
# # print(df.to_markdown())
#
# # Disconnect
# ib.disconnect()


import asyncio
import logging
from data_retriever import IBDataRetriever
from config import IB_HOST, IB_PORT, IB_CLIENT_ID

logger = logging.getLogger(__name__)


async def main():
    ib_data_retriever = IBDataRetriever(
        host=IB_HOST,
        port=IB_PORT,
        client_id=IB_CLIENT_ID)

    await ib_data_retriever.connect()

    # Define your symbol list
    symbols = ["AAPL", "TSLA", "AMZN"]

    # Option A: Retrieve historical data once and exit
    for sym in symbols:
        contract = ib_data_retriever.create_stock_contract(sym)
        df = await ib_data_retriever.fetch_historical_data(
            contract,
            duration_str="1 M",
            bar_size="1 hour",
            use_rth=True
        )
        # Save or process df as needed
        logger.info(f"Fetched {len(df)} bars for {sym}:\n{df.tail(5)}")

    # Option B: Continuous retrieval loop
    await ib_data_retriever.run_continuous_retrieval(symbols)

    # Done. Disconnect gracefully.
    ib_data_retriever.disconnect()

if __name__ == "__main__":
    # Run async entry point
    asyncio.run(main())
