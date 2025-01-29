# This is the base image needed to run all of my distributed code in Python

FROM python:3.12-slim

LABEL maintainer="Leismael Sosa <leismael.sosa17@gmail.com>"

# Basics commands for needed OS deppendencies
RUN apt-get update && \
    apt-get install -y fish

RUN apt-get install -y sudo
RUN apt-get install -y wget curl
RUN apt-get install -y net-tools inetutils-ping

# Optional
RUN apt-get install -y micro

RUN apt-get clean

# Basic Packages for Distributed Computing
RUN pip install --no-cache-dir scheduler Pyro5 pyzmq requests arrow

# Packages for Logging
RUN pip install --no-cache-dir eliot eliot-tree loguru

RUN pip install --no-cache-dir fastapi

RUN pip install --no-cache-dir rich[jupyter] tqdm click

RUN pip install --no-cache-dir aiohttp aiofiles cyclopts

# ------------------- Opentelemetry and FastAPI ---------------------------
# RUN apt install -y python3-dev && \
#     apt install -y build-essential
# RUN apt install -y build-essential

# RUN pip install opentelemetry-distro opentelemetry-exporter-otlp && \
#     opentelemetry-bootstrap -a install

# RUN pip install opentelemetry-api opentelemetry-sdk

# RUN pip install opentelemetry-instrumentation-fastapi
# -------------------------------------------------------------------------

# Create a home directory for the `fish terminal`
RUN useradd --create-home sosacode --password sosacode

# Add the user to the sudo group and grant sudo privileges
RUN usermod -aG sudo sosacode

# Set the password for the user 'sosacode' to be 'sosacode20'
RUN echo 'sosacode:sosacode20' | chpasswd

WORKDIR /app

USER sosacode

CMD ["fish"]
