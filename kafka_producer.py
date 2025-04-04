# producer.py
import json
import time
import argparse
from ib_insync import IB, Stock
from kafka import KafkaProducer
from config import IB_HOST, IB_PORT, IB_CLIENT_ID

def send_to_kafka(producer, topic, data):
    try:
        message_bytes = json.dumps(data).encode('utf-8')
        producer.send(topic, message_bytes)
        print(f"Sent to Kafka: {data}")
    except Exception as e:
        print(f"Kafka send failed: {e}")

def produce_historical(ib, producer, topic):
    contract = Stock('AAPL', 'SMART', 'USD')
    ib.qualifyContracts(contract)

    bars = ib.reqHistoricalData(
        contract, '', '1 D', '1 min', 'TRADES',
        useRTH=True, formatDate=1, keepUpToDate=False
    )

    for bar in bars:
        data_point = {
            "symbol": contract.symbol,
            "date": str(bar.date),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        }
        send_to_kafka(producer, topic, data_point)

def produce_realtime(ib, producer, topic):
    contract = Stock('AAPL', 'SMART', 'USD')
    ib.qualifyContracts(contract)
    ib.reqMktData(contract, '', False, False)

    def on_tick(tickers):
        for ticker in tickers:
            data_point = {
                "symbol": ticker.contract.symbol,
                "time": str(ticker.time),
                "last": ticker.last,
                "bid": ticker.bid,
                "ask": ticker.ask,
                "volume": ticker.volume
            }
            send_to_kafka(producer, topic, data_point)

    ib.pendingTickersEvent += on_tick
    ib.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['historical', 'realtime'], default='historical')
    args = parser.parse_args()

    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'marketdata'

    try:
        if args.mode == 'historical':
            produce_historical(ib, producer, topic)
        else:
            produce_realtime(ib, producer, topic)
    finally:
        producer.flush()
        producer.close()
        ib.disconnect()

if __name__ == '__main__':
    main()
