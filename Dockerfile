FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    python3.8 \
    python3.8-dev \
    python3.8-distutils \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3.8 get-pip.py \
    && rm get-pip.py \
    && python3.8 -m pip install --upgrade pip
RUN python3.8 -m pip install pdm==1.3.4
WORKDIR /src/datajoint-link
COPY . .
RUN pdm install -v --dev
ENTRYPOINT [ "pdm", "run" ]
