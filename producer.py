import time 
import json
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime


producer = KafkaProducer(
    bootstrap_servers =['127.0.0.1:9092'],
    api_version = (3, 3, 1),
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
print("Producer Started. Print Ctrl+c to Stop.")

while True:
    try:
        data = yf.Ticker("AAPL").history(period="1d",interval="1m")
        latest_row = data.iloc[-1]

        packet ={
            "ticker":"AAPL",
            "price": latest_row['Close'],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } 

        producer.send('stock-prices', value=packet)
        print(f"Sent Data to Kafka:{packet}")

        time.sleep(5)

    except Exception as e:
        print(f"An error occurred:{e}")
        time.sleep(5)