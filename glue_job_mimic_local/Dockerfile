FROM python:3.10-slim

# Install Java (use default-jdk instead)
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]