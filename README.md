# Wywy Website — Database Migrations

Liquibase runner for the Wywy website databases. Replaces the old Python
`create_tables` schema enforcer with versioned SQL changelogs.

Kept as submodule `apps/create_tables` inside Wywy-Website-Master-Database.

## Layout

- `changelog/wywywebsite.sql` — main data database (`wywywebsite`)
- `changelog/info.sql` — auth database (`users`, `sessions`)
- `entrypoint.sh` — container entrypoint (`migrate | seed`)
- `seed.py` — upserts the `admin` user into `info.users`

## Runtime

The container runs `entrypoint.sh` with one of:

- `migrate` (default) — runs Liquibase `update` once per database
- `seed` — upserts the `admin` user from `/run/secrets/admin`

### Environment variables

| Variable                     | Mode    | Value               |
| ---------------------------- | ------- | ------------------- |
| `DATABASE_HOST`              | both    | DB host             |
| `DATABASE_PORT`              | both    | DB port             |
| `DATABASE_MIGRATOR_USERNAME` | migrate | `wywy_migrator`     |
| `DATABASE_MIGRATOR_PASSWORD` | migrate | migrator credential |
| `DATABASE_USERNAME`          | seed    | `wywy_app`          |
| `DATABASE_PASSWORD`          | seed    | app credential      |

### Secrets

- `admin` — mounted at `/run/secrets/admin` (seed)

## Migrations

The changelog directory contains exactly two root files, one per database.
Never create additional files: the entrypoint passes `--changeLogFile` for
each database, and the flat formatted-SQL roots do not `--include` anything.

Future migrations APPEND new `--changeset wywy_migrator:<id>` blocks to the
same root file. Liquibase checksums are per-changeset, so appending is safe;
never modify the content of an already-applied changeset. A migration that
creates tables must also append a new `grants-<db>-<seq>` changeset granting
`wywy_app` DML on those tables, plus repeat `GRANT USAGE, SELECT ON ALL
SEQUENCES IN SCHEMA public` (it only covers sequences that exist at execution
time).

Roles, databases, extensions (postgis, postgres_fdw) and the sync_status FDW
machinery are superuser bootstrap concerns of the master-database repo, not of
this changelog.
