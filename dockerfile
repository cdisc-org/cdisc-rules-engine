FROM ubuntu:latest
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update && apt-get install -y software-properties-common curl && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
    python3.10 \
    python3.10-distutils \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.10 get-pip.py
COPY . /app
RUN mkdir -p /app/output && \
    chmod -R 777 /app
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyinstaller
EXPOSE 80
RUN chmod +x /app/tests/run_validation.sh
ENTRYPOINT ["/app/tests/run_validation.sh"]