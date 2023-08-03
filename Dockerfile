# Use a base image that has Java installed
FROM openjdk:8-jre-slim

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install PySpark
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the Python script and the data folder to the container's working directory
COPY wine-quality-prediction-inference.py .
COPY model model

# Default command to run your PySpark application
ENTRYPOINT ["python3", "wine-quality-prediction-inference.py"]