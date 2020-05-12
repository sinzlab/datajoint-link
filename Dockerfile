FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get update && apt-get install -y \
    curl \
    python3.8 \
    python3.8-distutils
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py &&\
    python3.8 get-pip.py &&\
    rm get-pip.py
RUN python3.8 -m pip install \
    jupyterlab \
    datajoint==0.12.5 \
    blackcellmagic \
    pytest \
    pytest-cov \
    docker \
    pymysql
COPY ./jupyter_notebook_config.py /root/.jupyter/
WORKDIR /src/link
COPY . .
RUN python3.8 -m pip install -e .
ENTRYPOINT ["jupyter", "lab", "--allow-root"]

