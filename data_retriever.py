import asyncio
import logging
import time
from ib_insync import IB, Stock, util, Contract
import pandas as pd
# from kafka import KafkaProducer
import json
import datetime

logger = logging.getLogger(__name__)


class IBDataRetriever:
    """
    A class for connecting to IB, fetching historical and real-time data,
    and handling reconnections automatically.
    """

    def __init__(self, host: str, port: int, client_id: int):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id

        # Reconnect attempts logic
        self.max_retries = 5  # max number of reconnection attempts
        self.retry_delay = 5  # seconds between attempts

    async def connect(self):
        """Establish the IB connection (asynchronously)."""
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Attempting to connect to IB Gateway/TWS: {self.host}:{self.port}, client_id={self.client_id}")
                await self.ib.connectAsync(host=self.host, port=self.port, clientId=self.client_id)
                logger.info("Connected to IB successfully.")
                return
            except ConnectionRefusedError as cre:
                logger.warning(f"Connection refused: {cre}; retrying in {self.retry_delay} seconds...")
                await asyncio.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Unexpected error on connect: {e}; retrying in {self.retry_delay} seconds...")
                await asyncio.sleep(self.retry_delay)

        raise ConnectionError("Max connection retries exceeded. Could not connect to IB.")

    def disconnect(self):
        """Disconnect from IB."""
        if self.ib.isConnected():
            logger.info("Disconnecting from IB...")
            self.ib.disconnect()
            logger.info("Disconnected.")

    async def ensure_connection(self):
        """
        Ensures the IB connection is alive; if not, tries to reconnect.
        """
        if not self.ib.isConnected():
            logger.info("IB is not connected. Attempting reconnect...")
            await self.connect()

    def create_stock_contract(self, symbol: str, currency: str = 'USD', exchange: str = 'SMART') -> Contract:
        """
        Create a Stock contract object.
        """
        return Stock(symbol, exchange, currency)


    def create_date_list(self, years_to_retrieve: int = 10):
        """
        Create a list of dates for each month to retrieve data using them as the end date
        """
        number_of_days = years_to_retrieve * 365

        date_list_fetch = [
            (datetime.datetime.today() - datetime.timedelta(days=30 * i)).strftime("%Y%m%d") + '-21:00:00' for i \
            in range(int(number_of_days / 30) + 1)]

        return date_list_fetch

    async def fetch_historical_data(
            self,
            contract: Contract,
            end_date_time: str = '',
            duration_str: str = '1 D',
            bar_size: str = '1 min',
            use_rth: bool = True
    ):
        """
        Retrieve historical data from IB. Returns a pandas DataFrame.
        """
        await self.ensure_connection()
        logger.info(f"Requesting historical data for {contract.localSymbol} [{duration_str}, {bar_size}]...")
        logger.info("NOTE: The Price Data is Mid-Price for OHLC.")
        bars_mid = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='MIDPOINT',
            useRTH=use_rth,
            formatDate=1,
        )

        bars_trd = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=use_rth,
            formatDate=1,
        )

        bars_ask = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='ASK',
            useRTH=use_rth,
            formatDate=1,
        )

        bars_bid = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='BID',
            useRTH=use_rth,
            formatDate=1,
        )

        bars_vol = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='OPTION_IMPLIED_VOLATILITY',
            useRTH=use_rth,
            formatDate=1,
        )

        df_mid = util.df(bars_mid)
        df_trd = util.df(bars_trd)
        df_ask = util.df(bars_ask)
        df_bid = util.df(bars_bid)
        df_vol = util.df(bars_vol)

        df_mid = df_mid[['date', 'open', 'high', 'low', 'close']]
        df_trd = df_trd[['date', 'volume', 'average', 'barCount']]
        df_ask = df_ask[['date', 'open', 'high', 'low', 'close']]
        df_bid = df_bid[['date', 'open', 'high', 'low', 'close']]
        df_vol = df_vol[['date', 'average']]
        df_vol = df_vol.rename(columns={'average': 'optvol'})

        df_spd = pd.merge(df_ask, df_bid, on='date', suffixes=('_ask', '_bid'))
        df_spd['spread'] = (df_spd['low_ask'] + df_spd['high_ask']) / 2 - \
                           (df_spd['low_bid'] + df_spd['high_bid']) / 2
        # Calculating Average Bid
        df_spd['avgbid'] = (df_spd['low_ask'] + df_spd['high_ask'] +
                            df_spd['low_bid'] + df_spd['high_bid']) / 4
        # Calculating Relative Spread
        df_spd['relspd'] = df_spd['spread'] / df_spd['avgbid']
        df_spd = df_spd[['date', 'avgbid', 'spread', 'relspd']]

        df = pd.merge(df_mid, df_trd, on='date')
        df = pd.merge(df, df_spd, on='date')
        df = pd.merge(df, df_vol, on='date')

        logger.info(f"Received {len(df)} rows of historical data for {contract.localSymbol}.")
        return df

    async def fetch_all_historical_data(self,
                                        contract: Contract,
                                        duration: str = '1',
                                        bar_size: str = '1 min',
                                        use_rth: bool = True
                                        ):
        """
        Fetches all the historical data for X number of years
        """

        await self.ensure_connection()
        duration = int(duration)
        logger.info(f"Subscribing to {duration}-year historical data for {contract.localSymbol}...")

        retrieval_date_list = self.create_date_list(duration)

        df = pd.DataFrame()
        for end_date in retrieval_date_list:
            logger.info(f"Starting to get data from {end_date}...")
            temp_df = await self.fetch_historical_data(contract=contract,
                                                       end_date_time=end_date,
                                                       duration_str='24 D',
                                                       bar_size=bar_size,
                                                       use_rth=use_rth)
            df = pd.concat([df, temp_df], ignore_index=True)

        df = df.drop_duplicates(ignore_index=True)

        return df


    async def fetch_real_time_data(self, contract: Contract):
        """
        Example of streaming real-time bars (5-second bars).
        This method sets up the subscription and returns a data queue (iterator).
        """
        await self.ensure_connection()
        logger.info(f"Subscribing to real-time bars for {contract.localSymbol}...")

        # Subscribe to real-time bars (5-second intervals)
        bars = self.ib.reqRealTimeBars(contract, whatToShow='TRADES', useRTH=False)

        # bars is an "ib_insync RealTimeBarList" which is updated in real-time.
        # We can create an async generator to yield updates as they come.
        async def bar_generator():
            while True:
                # The bars object will update in place; we can yield the latest bar or entire bars list
                yield bars[-1] if bars else None
                await asyncio.sleep(1)  # small sleep to avoid tight loop

        return bar_generator()

    @staticmethod
    def send_to_kafka(producer, topic, data):
        try:
            message_bytes = json.dumps(data).encode('utf-8')
            producer.send(topic, message_bytes)
            print(f"Sent to Kafka: {data}")
        except Exception as e:
            print(f"Kafka send failed: {e}")

    async def run_continuous_retrieval(self, symbols):
        """
        Example method that continuously retrieves historical data for a list of symbols.
        Could be triggered by a scheduler or run as a long-lived task.
        """
        while True:
            for sym in symbols:
                contract = self.create_stock_contract(sym)
                df = await self.fetch_historical_data(
                    contract,
                    duration_str='1 D',  # last 1 day
                    bar_size='1 min',
                    use_rth=False
                )
                # You would store the data in a DB or file here. For demo, let's just log the head.
                # logger.info(f"{sym} last min data {df[-1]}")
                # if not df.empty:
                #     logger.info(f"{sym} latest data:\n{df.tail(3)}")
                producer = KafkaProducer(bootstrap_servers='localhost:9092')
                topic = 'marketdata'
                if not df.empty:
                    # For demonstration, we take the last 3 rows.
                    data_to_send = df.tail(3)
                    # Convert DataFrame to JSON string.
                    json_message = data_to_send.to_json(orient='records')
                    # Send the JSON message to Kafka.
                    producer.send(topic, json_message.encode('utf-8'))
                    logger.info(f"{sym} latest data sent:\n{json_message}")

            # Sleep until next retrieval. Adjust as needed (e.g., 60 for once per minute, etc.).
            await asyncio.sleep(5)


