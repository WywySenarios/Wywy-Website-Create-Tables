FROM python:3.14.0-slim-bookworm
WORKDIR /apps/create_tables

COPY ./requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

COPY ./ .
COPY ./../../config.yml .

CMD ["python3", "create_tables.py"]