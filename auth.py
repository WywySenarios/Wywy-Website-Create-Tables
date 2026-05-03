import logging
import psycopg
from constants import CONN_CONFIG
from argon2 import PasswordHasher

logger = logging.getLogger()
password_hasher = PasswordHasher()


def ensure_auth_tables():
    with psycopg.connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    username            VARCHAR(32) UNIQUE NOT NULL,
                    email               TEXT UNIQUE,
                    password_hash       TEXT NOT NULL,
                    created_at          TIMESTAMP NOT NULL DEFAULT now(),
                    access_level        INTEGER NOT NULL,
                    tokens_remaining    DOUBLE PRECISION NOT NULL DEFAULT 1000,
                    last_refill         TIMESTAMP NOT NULL DEFAULT now(),
                    last_seen           TIMESTAMP NOT NULL DEFAULT now(),
                    
                    CHECK (access_level >= 0 AND access_level <= 100),
                    CHECK (username ~ '^[a-zA-Z0-9_-]+$'),
                    CHECK (char_length(username) >= 4)
                )
                """)
            logger.info("Table info/users is ready.")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id                  VARCHAR(24) PRIMARY KEY,
                    user_id             UUID REFERENCES users(id) NOT NULL,
                    secret_hash         VARCHAR(64) NOT NULL,
                    created_at          TIMESTAMP NOT NULL DEFAULT now(),
                    
                    CHECK (secret_hash ~ '^[0-9a-f]+$'),
                    CHECK (char_length(id) = 24),
                    CHECK (char_length(secret_hash) = 64)
                );
                """)
            logger.info("Table info/sessions is ready.")


def ensure_admin_user():
    with psycopg.connect(**CONN_CONFIG, dbname="info") as conn, open(
        "/run/secrets/admin", "r"
    ) as f:
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
