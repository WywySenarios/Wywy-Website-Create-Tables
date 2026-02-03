FROM python:3.14.0-slim-bookworm
# default build-time working directory
WORKDIR /apps/create_tables

# populate files (including requirements.txt)
COPY ./apps/create_tables .

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

CMD ["python3", "create_tables.py"]