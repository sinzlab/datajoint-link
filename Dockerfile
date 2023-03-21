FROM python:3.8
RUN pip install pdm
WORKDIR src
COPY . .
RUN python -m venv .venv \
    && pdm install --dev --no-lock
ENTRYPOINT [ "pdm", "run" ]
