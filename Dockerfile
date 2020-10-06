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
RUN python3.8 -m pip install \
    datajoint==0.12.5 \
    pytest \
    pytest-sugar \
    pytest-cov \
    pytest-xdist \
    docker \
    pymysql \
    minio \
    pep517
WORKDIR /src/link
COPY . .
RUN rm -rf dist \
 && python3.8 -m pep517.build . \
 && pip install dist/*.whl

