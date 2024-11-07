import requests
import json
from kafka import KafkaProducer


class WeatherAPI:
    def __init__(self,api_key,kafka_broker,topic):
        self.api_key =api_key
        self.base_url = "http://api.weatherapi.com/v1/current.json" # weather api url
        self.producer = KafkaProducer(
            bootstrap_servers =[kafka_broker],
            value_serializer = lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic

    def fetch_data(self,location):
        url = f"{self.base_url}?key={self.api_key}&q={location}"
        
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an HTTPError for bad response
            data = response.json()
        #sending data to kafka
            self.producer.send(self.topic,value=data)
            print(f"Sent weather data for {location} to Kafka topic: {self.topic}")
            return data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {location}: {e}")
            return None
 # main       
if __name__ == '__main__':
    api_key = '145ae39c1be84039b5c201211241510'
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    topic = 'weather'

    weather_api = WeatherAPI(api_key, kafka_broker, topic)
    locations = ['Toronto', 'Vancouver', 'Montreal', 'Calgary', 'Ottawa']

    for location in locations:
        weather_api.fetch_data(location)
