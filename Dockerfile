FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    curl \
    ffmpeg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN node --version && ffmpeg -version | head -1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only code — NO secrets
COPY main.py .
COPY auto_upload.py .

# Copy cookies if present
COPY youtube_cookies.tx[t] ./
COPY instagram_cookies.tx[t] ./
COPY cookies.tx[t] ./

# Tokens dir for OAuth pickles (populated at runtime)
RUN mkdir -p /app/tokens

EXPOSE 10000

CMD ["gunicorn", "main:app", "--workers", "2", "--timeout", "300", "--bind", "0.0.0.0:10000"]