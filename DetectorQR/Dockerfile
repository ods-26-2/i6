FROM debian:bookworm-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-gi gir1.2-gstreamer-1.0 \
    gstreamer1.0-plugins-base gstreamer1.0-plugins-bad \
    gstreamer1.0-tools libzbar0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv --system-site-packages /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir numpy opencv-contrib-python-headless pyzbar confluent-kafka
COPY leitor.py .
CMD ["python", "-u", "leitor.py"]   