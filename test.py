from data_retriever import IBDataRetriever
from config import IB_HOST, IB_PORT, IB_CLIENT_ID
import logging
import asyncio

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
            duration_str='2 Y',
            bar_size='1 min',
            what_to_show='TRADES',
            use_rth=True)

    print(df.to_markdown())
    print(df.info())

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