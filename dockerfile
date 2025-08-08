FROM ubuntu:latest
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install dependencies (keeping original tools)
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    unzip \
    jq \
    python3.12 \
    python3.12-venv \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download with better retry options but same structure
RUN LATEST_RELEASE_URL=$(curl -s --fail --retry 3 https://api.github.com/repos/cdisc-org/cdisc-rules-engine/releases/latest | jq -r '.assets[] | select(.name == "core-ubuntu-latest.zip") | .browser_download_url') \
    && echo "Downloading from: $LATEST_RELEASE_URL" \
    && curl -L \
        --fail \
        --retry 10 \
        --retry-delay 5 \
        --retry-max-time 1800 \
        --retry-connrefused \
        --retry-all-errors \
        --connect-timeout 30 \
        -C - \
        -o core-ubuntu-latest.zip \
        "$LATEST_RELEASE_URL" \
    && unzip core-ubuntu-latest.zip -d cdisc-rules-engine \
    && rm core-ubuntu-latest.zip \
    && ls -la cdisc-rules-engine/ \
    && ls -la cdisc-rules-engine/core/ \
    && mv cdisc-rules-engine/core/* . \
    && rm -rf cdisc-rules-engine \
    && chmod +x /app/core

CMD ["/bin/sh"]