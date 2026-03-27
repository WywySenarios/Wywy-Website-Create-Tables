import logging
import psycopg
from psycopg import sql
from constants import CONN_CONFIG
from config import CONFIG
from utils import select_result_is_true, to_lower_snake_case

logger = logging.getLogger()


def main():
    # create the info table
    with psycopg.connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            # ensure sync_status_enum exists
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_type WHERE typname = 'sync_status_enum'
                    ) THEN
                        CREATE TYPE sync_status_enum AS ENUM (
                            'already exists',
                            'added',
                            'mismatch',
                            'failed',
                            'anomalous'
                        );
                    END IF;
                END
                $$;
                """
            )

            # ensure info/sync_status exists
            cur.execute(
                """CREATE TABLE IF NOT EXISTS sync_status (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    parent_table_name TEXT NOT NULL,
                    table_type TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    remote_id TEXT NULL,
                    sync_timestamp TIMESTAMPTZ NULL,
                    status sync_status_enum NULL
                )"""
            )
            logger.info("Table info/sync_status is ready.")

    # ensure there are foreign tables
    # @TODO reserve table name "sync_status" inside create_tables code
    for databaseInfo in CONFIG["data"]:
        database_name = to_lower_snake_case(databaseInfo["dbname"])
        with psycopg.connect(**CONN_CONFIG, dbname=database_name) as data_conn:
            with data_conn.cursor() as data_cur:
                data_cur.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")

                # foreign tables server
                data_cur.execute(
                    """SELECT EXISTS (
                        SELECT 1
                        FROM pg_foreign_server
                        WHERE srvname = %s
                    );""",
                    ("sync_status_server",),
                )
                if not select_result_is_true(data_cur):
                    data_cur.execute(
                        sql.SQL(
                            """CREATE SERVER sync_status_server
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host {host}, dbname {database_name}, port {port});"""
                        ).format(
                            host=sql.Literal(CONN_CONFIG["host"]),
                            database_name=sql.Literal("info"),
                            port=sql.Literal(CONN_CONFIG["port"]),
                        )
                    )

                data_cur.execute(
                    sql.SQL(
                        """DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER sync_status_server;
                    CREATE USER MAPPING FOR PUBLIC
                    SERVER sync_status_server
                    OPTIONS (user {user}, password {password});"""
                    ).format(
                        user=sql.Literal(CONN_CONFIG["user"]),
                        password=sql.Literal(CONN_CONFIG["password"]),
                    )
                )

                data_cur.execute(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_type WHERE typname = 'sync_status_enum'
                        ) THEN
                            CREATE TYPE sync_status_enum AS ENUM (
                                'already exists',
                                'added',
                                'mismatch',
                                'failed',
                                'anomalous'
                            );
                        END IF;
                    END
                    $$;
                    """
                )

                data_cur.execute(
                    """DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_foreign_table ft
                            JOIN pg_class c ON ft.ftrelid = c.oid
                            WHERE c.relname = 'sync_status'
                        ) THEN
                            CREATE FOREIGN TABLE sync_status (
                                id INTEGER,
                                table_name TEXT,
                                parent_table_name TEXT,
                                table_type TEXT,
                                database_name TEXT,
                                entry_id TEXT,
                                remote_id TEXT,
                                sync_timestamp TIMESTAMPTZ,
                                status sync_status_enum
                            )
                            SERVER sync_status_server
                            OPTIONS (table_name 'sync_status');
                        END IF;
                    END
                    $$;
                    """
                )
                logger.info(f"Table {database_name}/sync_status is ready.")
