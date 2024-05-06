# Use the official Python image
FROM python:3.9

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app/
COPY requirements.txt /app/

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install confluent_kafka separately
RUN pip install confluent_kafka

# Copy the current directory contents into the container at /app/
COPY . /app/

# Expose the port that the app runs on
EXPOSE 8888

# Define environment variable for Kafka bootstrap servers
ENV KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.strimzi.svc.cluster.local:9092

# Run the Flask app when the container launches
CMD ["python3", "app.py"]
