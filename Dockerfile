# Use an official Python base image
FROM python:3.10-slim


# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY config.py .
COPY mqtt_kafka_bridge.py .


# Set the entry point to run the bridge
ENTRYPOINT ["python", "mqtt_kafka_bridge.py"]
