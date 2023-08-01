# Use the official Python image as the base image
FROM python:3.9.5

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script and the data folder to the container's working directory
COPY wine-quality-prediction-inference.py .
COPY model model

# Install any required dependencies for your Python script
# For example, if you have requirements.txt, uncomment the next line
 COPY requirements.txt .

# Install dependencies (if using requirements.txt)
 RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run your Python script
CMD ["python", "wine-quality-prediction-inference.py"]
