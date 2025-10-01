FROM --platform=linux/amd64 mcr.microsoft.com/devcontainers/python:1-3.12-bullseye as base

RUN touch /etc/apt/sources.list.d/amd64.list
RUN dpkg --add-architecture amd64
RUN apt update

# NO extra sources files, NO dpkg --add-architecture
RUN apt-get update \
     && apt-get -y install --no-install-recommends \
     binutils \
     libwebkit2gtk-4.0-dev \
     sqlite3 \
     && rm -rf /var/lib/apt/lists/*

# Install `just`
RUN pipx install rust-just

# Install uv and increase the timeout (in seconds) for HTTP requests
RUN pipx install uv
ENV UV_HTTP_TIMEOUT=300 
ENV UV_LINK_MODE=copy

WORKDIR /workspaces/QSAutomate

# Copy .env file and dependency files
COPY .env pyproject.toml uv.lock* ./

# Source env vars and install dependencies
RUN export $(cat .env | grep -v '^#' | xargs) && \
     git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/" && \
     uv export --no-dev --no-hashes -o /tmp/req.txt && \
     pip install --no-cache-dir -r /tmp/req.txt

# Create MLflow directories
RUN mkdir -p ~/.qsresearch/mlflow
