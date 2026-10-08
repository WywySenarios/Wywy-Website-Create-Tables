"""Seed the admin user into the info database.

Reads the admin password from /run/secrets/admin, argon2-hashes it, and
upserts the 'admin' user (access_level 100) into info.users.

Port of auth.py's ensure_admin_user() minus table creation: tables are created
by Liquibase migrations, and this runs as master_db_app (DML-only).
"""

import logging
import os

import psycopg
from argon2 import PasswordHasher

logger = logging.getLogger()
password_hasher = PasswordHasher()


def ensure_admin_user():
    with (
        psycopg.connect(
            host=os.environ["DATABASE_HOST"],
            port=os.environ["DATABASE_PORT"],
            dbname="info",
            user=os.environ["DATABASE_USERNAME"],
            password=os.environ["DATABASE_PASSWORD"],
            sslmode="prefer",
        ) as conn,
        open("/run/secrets/admin", "r") as f,
    ):
        with conn.cursor() as cur:
            # @TODO admin email
            cur.execute(
                """
                INSERT INTO users (username, password_hash, access_level) VALUES ('admin', %s, 100) ON CONFLICT (username) DO UPDATE SET
                    email = NULL,
                    password_hash = EXCLUDED.password_hash,
                    access_level = 100;
                """,
                (password_hasher.hash(f.read()),),
            )
            logger.info("The admin user is ready.")


if __name__ == "__main__":
    ensure_admin_user()
