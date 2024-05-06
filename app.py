from flask import Flask, render_template
from confluent_kafka import Consumer, KafkaError
import json
import threading

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'my-cluster-kafka-bootstrap.strimzi.svc.cluster.local:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Kafka topic to consume messages from
topic = 'productclick'

# Initialize Flask app
app = Flask(__name__)

# List to store received messages
received_messages = []

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
    # Append the message to the received_messages list
    received_messages.append(event_data)
    # Print the received message
    print("Received event:", event_data)

@app.route('/')
def index():
    # Run the Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.start()
    
    # Render the template with received_messages
    return render_template('index.html', messages=received_messages)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8888)
