# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies if required by packages like duckdb or pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose port 5000 for the app
EXPOSE 5000

# Set environment variable for Flask
ENV FLASK_ENV=production

# Run the app using Waitress (a production-ready WSGI server listed in your requirements)
CMD ["waitress-serve", "--port=5000", "app:app"]
