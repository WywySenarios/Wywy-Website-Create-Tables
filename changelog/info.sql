--liquibase formatted sql

--changeset master_db_migrator:baseline-info
-- Baseline for the info database, exactly as auth.py defined it (auth.py was
-- the executable spec). Plain CREATE TABLE statements only. No GRANTs here
-- (grants changeset below).

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(32) UNIQUE NOT NULL,
    email TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    access_level INTEGER NOT NULL,
    tokens_remaining DOUBLE PRECISION NOT NULL DEFAULT 1000,
    last_refill TIMESTAMP NOT NULL DEFAULT now(),
    last_seen TIMESTAMP NOT NULL DEFAULT now(),

    CHECK (access_level >= 0 AND access_level <= 100),
    CHECK (username ~ '^[a-zA-Z0-9_-]+$'),
    CHECK (char_length(username) >= 4),
    CHECK (tokens_remaining > -0.001)
);

CREATE TABLE sessions (
    id VARCHAR(24) PRIMARY KEY,
    user_id UUID REFERENCES users (id) NOT NULL,
    secret_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    last_seen TIMESTAMP NOT NULL DEFAULT now(),

    CHECK (secret_hash ~ '^[0-9a-f]+$'),
    CHECK (char_length(id) = 24),
    CHECK (char_length(secret_hash) = 64)
);

--rollback DROP TABLE IF EXISTS sessions;
--rollback DROP TABLE IF EXISTS users;

--changeset master_db_migrator:grants-info-1
-- DML grants for master_db_app. Explicit per-table GRANTs, never ON ALL TABLES.
GRANT SELECT, INSERT, UPDATE, DELETE ON users, sessions TO master_db_app;
-- Executes now as a no-op (info has no sequences at baseline: UUID/VARCHAR
-- PKs). It grants only sequences that exist at execution time. A FUTURE info
-- migration adding a SERIAL column MUST add a new grants-info-<seq> changeset
-- that repeats this grant.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO master_db_app;
-- Defensive no-op on a clean-slate DB: see wywywebsite.sql grants changeset.
REVOKE ALL PRIVILEGES ON TABLE public.DATABASECHANGELOG, public.DATABASECHANGELOGLOCK FROM master_db_app;
--rollback REVOKE SELECT, INSERT, UPDATE, DELETE ON users, sessions FROM master_db_app;
--rollback REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM master_db_app;
-- Rollback of the defensive REVOKE is a deliberate NO-OP (same REVOKE), not an
-- inverse: see wywywebsite.sql grants changeset.
--rollback REVOKE ALL PRIVILEGES ON TABLE public.DATABASECHANGELOG, public.DATABASECHANGELOGLOCK FROM master_db_app;
