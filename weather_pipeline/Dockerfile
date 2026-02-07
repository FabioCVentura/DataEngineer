# Base image - Uses a lightweight Python 3.13 image. 'slim' keeps the image size small
FROM python:3.13-slim

# Working directory inside the container
WORKDIR /weather_pipeline

# Environment variables
# PYTHONPATH ensures Python can import the 'src/' modules
# PYTHONUNBUFFERED=1 makes print/log statements appear immediately (no buffering), which is critical for real-time logs in pipelines

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Install dependencies - Copy requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install Python packages listed in requirements.txt
# --no-cache-dir avoids keeping pip cache, reducing image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into the container, resulting in a self-contained solution
COPY src/ src/
COPY config/ config/
COPY data/ data/
COPY tests/ tests/


# Optional: Volume for persistent data even if the container is destroyed
VOLUME /weather_pipeline/data

# Pipeline execution - 'python -m src.main' ensures the pipeline executes in a self-contained way
CMD ["python", "-m", "src.main"]
