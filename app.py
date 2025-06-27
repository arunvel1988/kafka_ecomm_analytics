from flask import Flask, render_template
from confluent_kafka import Consumer, KafkaError
import json
import threading
import time

from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Kafka topic to consume messages from
topic = 'productclick'

# Initialize Flask app
app = Flask(__name__)

# SQLAlchemy setup
engine = create_engine('sqlite:///product_clicks.db')
Base = declarative_base()

class ProductClick(Base):
    __tablename__ = 'product_clicks'

    id = Column(Integer, primary_key=True)
    product_id = Column(String)
    timestamp = Column(Integer)  # Unix timestamp

    def __repr__(self):
        return f"ProductClick(product_id='{self.product_id}', timestamp={self.timestamp})"

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def kafka_consumer():
    # Create Kafka consumer instance
    consumer = Consumer(conf)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for new messages from Kafka
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print('%% %s [%d] reached end at offset %d\n' %
                          (message.topic(), message.partition(), message.offset()))
                elif message.error():
                    raise KafkaException(message.error())
            else:
                # Process the received message
                process_message(message.value())

    finally:
        # Close the consumer on exit
        consumer.close()

def process_message(message):
    # Decode the message from JSON
    event_data = json.loads(message.decode('utf-8'))

    # Extract the product ID and timestamp from the event data
    product_id = event_data.get('product_id')
    timestamp = int(time.time())  # Unix timestamp

    # Create a new session
    session = Session()

    # Create a new ProductClick instance and add it to the session
    product_click = ProductClick(product_id=product_id, timestamp=timestamp)
    session.add(product_click)

    # Commit the changes and close the session
    session.commit()
    session.close()

    # Print the received message
    print("Received event:", event_data)

@app.route('/')
def index():
    # Run the Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.start()

    return render_template('index.html')

@app.route('/analytics')
def analytics():
    # Create a new session
    session = Session()

    # Get all product clicks with click count
    product_clicks_data = (
        session.query(ProductClick.product_id, func.count(ProductClick.id).label('click_count'))
        .group_by(ProductClick.product_id)
        .all()
    )

    # Convert the query result to a dictionary
    product_clicks_dict = {
        product_id: click_count for product_id, click_count in product_clicks_data
    }

    # Close the session
    session.close()

    # Render the template with the product clicks data
    return render_template('analytics.html', product_clicks_data=product_clicks_dict)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8888)
