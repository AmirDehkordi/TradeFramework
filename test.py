from data_retriever import IBDataRetriever
from config import IB_HOST, IB_PORT, IB_CLIENT_ID
import logging
import asyncio
import pandas as pd

logger = logging.getLogger()


async def test():
    test_ib = IBDataRetriever(
        host=IB_HOST,
        port=IB_PORT,
        client_id=IB_CLIENT_ID)

    await test_ib.connect()

    symbol = 'AAPL'
    symcon = test_ib.create_stock_contract(symbol)

    df = await test_ib.fetch_historical_data(
        contract=symcon,
        end_date_time='',
        duration_str='1 D',
        bar_size='1 min',
        use_rth=True)

    # df_trd = await test_ib.fetch_historical_data(
    #     contract=symcon,
    #     end_date_time='',
    #     duration_str='1 D',
    #     bar_size='1 min',
    #     what_to_show='TRADES',
    #     use_rth=True)
    #
    # df_ask = await test_ib.fetch_historical_data(
    #     contract=symcon,
    #     end_date_time='',
    #     duration_str='1 D',
    #     bar_size='1 min',
    #     what_to_show='ASK',
    #     use_rth=True)
    #
    # df_bid = await test_ib.fetch_historical_data(
    #     contract=symcon,
    #     end_date_time='',
    #     duration_str='1 D',
    #     bar_size='1 min',
    #     what_to_show='BID',
    #     use_rth=True)
    #
    # df_mid = df_mid[['date', 'open', 'high', 'low', 'close']]
    # df_trd = df_trd[['date', 'volume', 'average', 'barCount']]
    # df_ask = df_ask[['date', 'open', 'high', 'low', 'close']]
    # df_bid = df_bid[['date', 'open', 'high', 'low', 'close']]
    #
    # df_spd = pd.merge(df_ask, df_bid, on='date', suffixes=('_ask', '_bid'))
    # df_spd['spread'] = (df_spd['low_ask'] + df_spd['high_ask']) / 2 - \
    #                    (df_spd['low_bid'] + df_spd['high_bid']) / 2
    # # Calculating Average Bid
    # df_spd['avgbid'] = (df_spd['low_ask'] + df_spd['high_ask'] +
    #                     df_spd['low_bid'] + df_spd['high_bid']) / 4
    # # Calculating Relative Spread
    # df_spd['relspd'] = df_spd['spread'] / df_spd['avgbid']
    # df_spd = df_spd[['date', 'avgbid', 'spread', 'relspd']]
    #
    # df = pd.merge(df_mid, df_trd, on='date')
    # df = pd.merge(df, df_spd, on='date')

    df = df.set_index('date')
    print(df.to_markdown())
    print(df.info())
    df.to_csv(r'C:\q\w64\aapl_recent.csv')

    test_ib.disconnect()


asyncio.run(test())

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
