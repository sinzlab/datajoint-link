FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    graphviz \
    python3.8 \
    python3.8-dev \
    python3.8-distutils \
    git \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3.8 get-pip.py \
    && rm get-pip.py \
    && python3.8 -m pip install --upgrade pip
RUN python3.8 -m pip install pdm==2.1
WORKDIR /src/datajoint-link
COPY . .
# Fix for https://github.com/actions/virtual-environments/issues/2803
ENV LD_PRELOAD=/lib/x86_64-linux-gnu/libgcc_s.so.1
RUN pdm sync --dev --group profiling
ENTRYPOINT [ "pdm", "run" ]
