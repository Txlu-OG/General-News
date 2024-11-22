from confluent_kafka import Producer
import json
import requests
import time

# News API key and endpoint
api_key = "9c40dfed39574e099b7f036dcbf30ed8"
base_url = "https://newsapi.org/v2/top-headlines"
country = "us"
category = "business"

# Kafka configuration
kafka_topics = {
    'google_news': 'news_google',
    'cnn_news': 'news_cnn'
}
kafka_bootstrap_servers = 'broker:29092'

# Create a Kafka producer
producer_conf = {'bootstrap.servers': kafka_bootstrap_servers}
producer = Producer(**producer_conf)

def delivery_report(err, msg):
    """Delivery report for produced messages"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def get_news_data(source):
    """Fetch news data from the API."""
    try:
        url = f"{base_url}?country={country}&category={category}&apiKey={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news data: {e}")
        return None

def fetch_and_send_news_data():
    """Fetch and send news data to Kafka."""
    sources = {
        'google_news': 'google_news',
        'cnn_news': 'cnn_news'
    }
    
    for source, topic in sources.items():
        news_data = get_news_data(source)
        if news_data:
            producer.produce(kafka_topics[topic], value=json.dumps(news_data), callback=delivery_report)
            producer.flush()
            print(f"Sent news data from {source} to Kafka topic {kafka_topics[topic]}")

if __name__ == "__main__":
    while True:
        fetch_and_send_news_data()
        time.sleep(60 * 10)  # Fetch news every 10 minutes
