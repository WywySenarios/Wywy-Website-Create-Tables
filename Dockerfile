FROM python:3.14.0-slim-bookworm
# default build-time working directory
WORKDIR /apps/create_tables

# bulid parameters
ARG CONFIG_PATH="./../../config.yml"

# populate files (including requirements.txt)
COPY . .
COPY ${CONFIG_PATH} .

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

CMD ["python3", "create_tables.py"]