# consumer_signals.py
import json
from kafka import KafkaConsumer

def process_signal(data):
    # Example signal: print when price is above 200
    if data.get('close') and data['close'] > 200:
        print(f"Signal! {data['symbol']} close is {data['close']}")

def main():
    consumer = KafkaConsumer(
        'marketdata',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='signal-processing-group'
    )

    print("Consumer started, listening for data...")

    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))
            print(f"Received: {data}")
            process_signal(data)
        except json.JSONDecodeError:
            print("Invalid JSON received.")
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == '__main__':
    main()
