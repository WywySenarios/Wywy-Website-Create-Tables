Automatic table creation based on a YAML config.

The filepath to the YAML config should be supplied via build-time argument CONFIG_PATH.

The following environment variables need to be included:

- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USERNAME
- DATABASE_PASSWORD
- DATABASE_TYPE, which describes the types of tables to create.
