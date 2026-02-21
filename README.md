Automatic table creation based on a YAML config.

The YAML config should be supplied via docker volume.

The following environment variables need to be included:

- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USERNAME
- DATABASE_PASSWORD
- SYNC_STATUS, which describes whether or not the sync status table should be created. Defaults to False.
