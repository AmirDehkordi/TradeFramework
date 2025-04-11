from data_retriever import IBDataRetriever
from config import IB_HOST, IB_PORT, IB_CLIENT_ID
import logging
import asyncio
import datetime
import pandas as pd
import os

logger = logging.getLogger()


async def test():
    fetch_ib = IBDataRetriever(
        host=IB_HOST,
        port=IB_PORT,
        client_id=IB_CLIENT_ID)

    await fetch_ib.connect()

    spx50_list = pd.read_csv('SPX50.csv')['Ticker']
    for symbol in spx50_list:
        symbol = symbol
        bar_size = '1 day'  # or 1 min / 1 hour
        symbol_file_name = fr'D:\SPX50\{symbol}-1-{bar_size.split()[-1]}.csv'
        symcon = fetch_ib.create_stock_contract(symbol)

        # Check if the file exists (Update or Initialize)
        if os.path.exists(symbol_file_name):
            print(f'Updating data for {symbol}')
            temp_df = pd.read_csv(symbol_file_name)
            last_date_available = temp_df['date'].values[-1]
            logger.info(f"The file for {symbol} exists. Updating the file from {last_date_available}...")
            update_df = await fetch_ib.fetch_historical_data(symcon,
                                                             duration_str="3 D",
                                                             bar_size=bar_size,
                                                             use_rth=True)
            update_df = pd.concat([temp_df, update_df], ignore_index=True)
            update_df = update_df.drop_duplicates(subset=['date'])
            update_df.to_csv(symbol_file_name)
        else:
            print(f'Fetching historical data for {symbol}')
            logger.info(f"The file for {symbol} does not exist. Fetching all the historical data from 2020-01-01...")
            df = await fetch_ib.fetch_all_historical_data(contract=symcon,
                                                          start_year=2024,
                                                          bar_size=bar_size,
                                                          use_rth=True)
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] >= '2020-01-01']
            df = df.set_index('date')
            df.to_csv(symbol_file_name)

    fetch_ib.disconnect()


asyncio.run(test())
