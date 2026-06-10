FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends kcov && rm -rf /var/lib/apt/lists/*
