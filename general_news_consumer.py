import json
import psycopg2 as psycopg
import logging
from confluent_kafka import Consumer, KafkaException
from psycopg2 import OperationalError
import time

# Set up logging
logging.basicConfig(level=logging.INFO)

def init_conn():
    db_url = (
        'postgresql://localhost:26257/defaultdb?'
        'sslrootcert=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/ca.crt&'
        'sslkey=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/client.ukeme.key.pk8&'
        'sslcert=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/client.ukeme.crt&'
        'sslmode=verify-full&'
        'user=ukeme&password=cockroach'
    )
    try:
        conn = psycopg.connect(db_url, application_name="kafka-cockroach illustration")
        logging.info("Database connection established successfully.")
        return conn
    except OperationalError as e:
        logging.fatal(f"Database connection failed: {e}")
        raise

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS news_data (
        source_name TEXT,
        author TEXT,
        title TEXT,
        description TEXT,
        url TEXT,
        url_to_image TEXT,
        published_at TIMESTAMP,
        content TEXT
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table 'news_data' created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise
    finally:
        cursor.close()

def insert_news_data(source, news_data):
    conn = init_conn()
    create_table_if_not_exists(conn)
    cursor = conn.cursor()
    try:
        insert_query = """
        INSERT INTO news_data (
            source_name, author, title, description, url, url_to_image, published_at, content
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        articles = news_data.get('articles', [])
        for article in articles:
            values = (
                source,
                article.get('author', ''),
                article.get('title', ''),
                article.get('description', ''),
                article.get('url', ''),
                article.get('urlToImage', ''),
                article.get('publishedAt', ''),
                article.get('content', '')
            )
            cursor.execute(insert_query, values)
        conn.commit()
        logging.info(f"Inserted news data from {source} into CockroachDB.")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

# Kafka configuration
kafka_topics = ['news_google', 'news_cnn']
kafka_bootstrap_servers = 'broker:29092,broker:39092,broker:49092'

# Create a Kafka consumer
consumer = Consumer({
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'news-data-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(kafka_topics)

def consume_and_store_data():
    source_names = {
        'news_google': 'Google News',
        'news_cnn': 'CNN News'
    }
    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        
        source = source_names.get(msg.topic(), 'Unknown Source')
        news_data = json.loads(msg.value().decode('utf-8'))
        insert_news_data(source, news_data)

if __name__ == "__main__":
    consume_and_store_data()
