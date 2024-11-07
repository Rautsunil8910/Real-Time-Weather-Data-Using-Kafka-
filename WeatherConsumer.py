from kafka import KafkaConsumer
import json
from pymongo import MongoClient


class WeatherConsumer:
    
    def __init__(self, kafka_broker, topic, db_uri, db_name):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]

    def consume(self):
        print(f"Starting to consume messages from Kafka topic: {self.consumer.subscription()}")
        for message in self.consumer:
            weather_data = message.value
            # Process and store the weather data in MongoDB
            self.store_data(weather_data)

    def store_data(self, data):
        self.db.weather.insert_one(data)
        print(f"Stored data for {data['location']['name']} in MongoDB")


if __name__ == '__main__':
    kafka_broker = 'localhost:9092'
    topic = 'weather'
    db_uri = "mongodb+srv://BDAT1004:qJpw3jz1jcTD6K2T@cluster0.tnchmwe.mongodb.net/weather_db?ssl=true&ssl_cert_reqs=CERT_NONE"  # MongoDB Atlas URI
    db_name = 'weatherAPI'

    weather_consumer = WeatherConsumer(kafka_broker, topic, db_uri, db_name)
    weather_consumer.consume()