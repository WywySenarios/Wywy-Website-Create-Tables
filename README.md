Automatic table creation based on a YAML config.

The YAML config should be supplied via docker volume.

The following environment variables need to be included:

- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USERNAME
- DATABASE_PASSWORD
- DATABASE_TYPE, which describes the types of tables to create.
