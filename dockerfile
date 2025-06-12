FROM ubuntu:latest
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
# Install dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    unzip \
    jq \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y \
    python3.12 \
    python3.12-distutils \
    && rm -rf /var/lib/apt/lists/*
# Download the latest release
RUN LATEST_RELEASE_URL=$(curl -s https://api.github.com/repos/cdisc-org/cdisc-rules-engine/releases/latest | jq -r '.assets[] | select(.name == "core-ubuntu-latest.zip") | .browser_download_url') \
    && curl -L -o core-ubuntu-latest.zip "$LATEST_RELEASE_URL" \
    && unzip core-ubuntu-latest.zip -d cdisc-rules-engine \
    && rm core-ubuntu-latest.zip \
    && mv cdisc-rules-engine/* . \
    && rmdir cdisc-rules-engine \
    && chmod +x /app/core
CMD ["/bin/sh"]