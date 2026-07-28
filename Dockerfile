FROM python:3.14.0-slim-bookworm

# create non-root user for runtime
ARG USER_ID
RUN groupadd -g 2523 Wywy-Website
RUN useradd -g Wywy-Website -u ${USER_ID} -m -d /home/create_tables create_tables

# populate requirements.txt
COPY --chown=create_tables:Wywy-Website /apps/create_tables/requirements.txt /home/create_tables/requirements.txt

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Download config.yml for CI (no runtime mount needed).
RUN curl -fsSL https://raw.githubusercontent.com/WywySenarios/Wywy-Website-Control/main/config/config.yml \
    -o /home/create_tables/config.yml

# Create log directory so CI works without a host mount.
RUN mkdir -p /var/log/Wywy-Website/create_tables && \
    chown -R create_tables:Wywy-Website /var/log/Wywy-Website

RUN pip install -r /home/create_tables/requirements.txt

# populate all files
COPY --chown=create_tables:Wywy-Website /apps/create_tables /home/create_tables

USER create_tables

ENTRYPOINT ["python3"]
CMD ["/home/create_tables/create_tables.py"]