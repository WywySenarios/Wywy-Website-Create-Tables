Automatic table creation based on a YAML config.

The YAML config should be supplied via docker volume to the destination /home/create_tables/config.yml .

The following build-time variables (args) need to be included:

- USER_ID

The following environment variables need to be included:

- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USERNAME
- DATABASE_PASSWORD
- SYNC_STATUS, which describes whether or not the sync status table should be created. Defaults to False.
