from flask import Flask, render_template, send_file
from kafka import KafkaConsumer
import json
import threading
import time
import csv
import os

from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.orm import declarative_base, sessionmaker

# Kafka consumer configuration
KAFKA_BROKER = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
TOPIC = 'productclick'

# Flask app
app = Flask(__name__)

# SQLAlchemy setup
engine = create_engine('sqlite:///product_clicks.db')
Base = declarative_base()

class ProductClick(Base):
    __tablename__ = 'product_clicks'

    id = Column(Integer, primary_key=True)
    product_id = Column(String)
    timestamp = Column(Integer)

    def __repr__(self):
        return f"ProductClick(product_id='{self.product_id}', timestamp={self.timestamp})"

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Kafka consumer thread
def kafka_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='my-consumer-group',
        auto_offset_reset='earliest',  # Change to 'latest' if needed
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        process_message(message.value)

def process_message(event_data):
    product_id = event_data.get('product_id')
    timestamp = int(time.time())

    session = Session()
    product_click = ProductClick(product_id=product_id, timestamp=timestamp)
    session.add(product_click)
    session.commit()
    session.close()

    print("Received event:", event_data)

@app.route('/')
def index():
    # Start consumer thread only once
    if not any(t.name == "KafkaThread" for t in threading.enumerate()):
        kafka_thread = threading.Thread(target=kafka_consumer, name="KafkaThread", daemon=True)
        kafka_thread.start()
    return render_template('index.html')

@app.route('/analytics')
def analytics():
    session = Session()
    product_clicks_data = (
        session.query(ProductClick.product_id, func.count(ProductClick.id).label('click_count'))
        .group_by(ProductClick.product_id)
        .all()
    )
    session.close()

    product_clicks_dict = {product_id: click_count for product_id, click_count in product_clicks_data}
    return render_template('analytics.html', product_clicks_data=product_clicks_dict)

# ✅ New endpoint to export SQLite table to CSV
@app.route('/export_csv')
def export_csv():
    session = Session()
    clicks = session.query(ProductClick).all()
    session.close()

    csv_file_path = 'product_clicks.csv'
    with open(csv_file_path, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'timestamp'])  # Header
        for click in clicks:
            writer.writerow([click.product_id, click.timestamp])

    # Send CSV file as download
    return send_file(csv_file_path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8888)
