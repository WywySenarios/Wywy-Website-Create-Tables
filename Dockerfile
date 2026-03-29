FROM python:3.14.0-slim-bookworm

# create non-root user for runtime
ARG USER_ID
RUN groupadd -g 2523 Wywy-Website
RUN useradd -g Wywy-Website -u ${USER_ID} -m -d /home/create_tables create_tables

# populate files (including requirements.txt)
COPY --chown=create_tables:Wywy-Website /apps/create_tables /home/create_tables

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r /home/create_tables/requirements.txt

USER create_tables

ENTRYPOINT ["python3"]
CMD ["/home/create_tables/create_tables.py"]