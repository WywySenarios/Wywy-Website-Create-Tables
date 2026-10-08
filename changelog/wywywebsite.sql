--liquibase formatted sql

--changeset master_db_migrator:baseline-wywywebsite
-- Baseline for the wywywebsite database, written by hand from config.yml
-- semantics as enforced by the old create_tables.py (the executable spec).
-- Plain CREATE TABLE / CREATE INDEX only. No GRANTs here (grants changeset
-- below). No procedural content. Creation order is FK-safe.

CREATE TABLE location (
    id SERIAL PRIMARY KEY,
    latitude real,
    longitude real,
    accuracy real,
    altitude real,
    altitude_accuracy real,
    speed real,
    heading real,
    "timestamp" timestamp
);

CREATE TABLE daily (
    id SERIAL PRIMARY KEY,
    "date" timestamp,
    work_ethic integer,
    work_ethic_comments text DEFAULT '',
    time_efficiency integer,
    time_efficiency_comments text DEFAULT '',
    happiness integer,
    happiness_comments text DEFAULT '',
    awareness integer,
    awareness_comments text DEFAULT ''
);

CREATE TABLE events_tag_names (
    id SERIAL PRIMARY KEY,
    tag_name TEXT NOT NULL UNIQUE
);

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    primary_tag INT NOT NULL CONSTRAINT fk_primary_tags REFERENCES events_tag_names (id),
    start_time timestamp,
    end_time timestamp,
    start_location geography(POINT, 4326),
    start_location_latlong_accuracy double precision,
    start_location_altitude double precision,
    start_location_altitude_accuracy double precision,
    end_location geography(POINT, 4326),
    end_location_latlong_accuracy double precision,
    end_location_altitude double precision,
    end_location_altitude_accuracy double precision
);

CREATE TABLE events_tags (
    id SERIAL PRIMARY KEY,
    entry_id INT NOT NULL REFERENCES events (id),
    tag_id INT NOT NULL REFERENCES events_tag_names (id)
);

CREATE TABLE events_tag_aliases (
    alias TEXT PRIMARY KEY,
    tag_id INT NOT NULL REFERENCES events_tag_names (id)
);

CREATE TABLE events_tag_groups (
    id SERIAL PRIMARY KEY,
    tag_id INT NOT NULL REFERENCES events_tag_names (id),
    group_name TEXT NOT NULL
);

CREATE TABLE events_sleep_descriptors (
    id SERIAL PRIMARY KEY,
    sleep_quality integer,
    sleep_quality_comments text DEFAULT '',
    sleep_behaviour integer,
    sleep_behaviour_comments text DEFAULT ''
);

CREATE TABLE thought_journal_tag_names (
    id SERIAL PRIMARY KEY,
    tag_name TEXT NOT NULL UNIQUE
);

CREATE TABLE thought_journal (
    id SERIAL PRIMARY KEY,
    primary_tag INT NOT NULL CONSTRAINT fk_primary_tags REFERENCES thought_journal_tag_names (id),
    thought_name text,
    thought text
);

CREATE TABLE thought_journal_tags (
    id SERIAL PRIMARY KEY,
    entry_id INT NOT NULL REFERENCES thought_journal (id),
    tag_id INT NOT NULL REFERENCES thought_journal_tag_names (id)
);

CREATE TABLE thought_journal_tag_aliases (
    alias TEXT PRIMARY KEY,
    tag_id INT NOT NULL REFERENCES thought_journal_tag_names (id)
);

CREATE TABLE thought_journal_tag_groups (
    id SERIAL PRIMARY KEY,
    tag_id INT NOT NULL REFERENCES thought_journal_tag_names (id),
    group_name TEXT NOT NULL
);

CREATE TABLE thought_journal_related_item_descriptors (
    id SERIAL PRIMARY KEY,
    related_item integer,
    related_item_type character varying(64)
);

--rollback DROP TABLE IF EXISTS thought_journal_related_item_descriptors;
--rollback DROP TABLE IF EXISTS thought_journal_tag_groups;
--rollback DROP TABLE IF EXISTS thought_journal_tag_aliases;
--rollback DROP TABLE IF EXISTS thought_journal_tags;
--rollback DROP TABLE IF EXISTS thought_journal;
--rollback DROP TABLE IF EXISTS thought_journal_tag_names;
--rollback DROP TABLE IF EXISTS events_sleep_descriptors;
--rollback DROP TABLE IF EXISTS events_tag_groups;
--rollback DROP TABLE IF EXISTS events_tag_aliases;
--rollback DROP TABLE IF EXISTS events_tags;
--rollback DROP TABLE IF EXISTS events;
--rollback DROP TABLE IF EXISTS events_tag_names;
--rollback DROP TABLE IF EXISTS daily;
--rollback DROP TABLE IF EXISTS location;

--changeset master_db_migrator:grants-wywywebsite-1
-- DML grants for master_db_app. Explicit per-table GRANTs, never ON ALL TABLES
-- (a blanket grant would leak DML onto Liquibase's bookkeeping tables).
GRANT SELECT, INSERT, UPDATE, DELETE ON
    location, events, events_tag_names, events_tags, events_tag_aliases, events_tag_groups,
    events_sleep_descriptors, daily, thought_journal,
    thought_journal_tag_names, thought_journal_tags, thought_journal_tag_aliases, thought_journal_tag_groups,
    thought_journal_related_item_descriptors TO master_db_app;
-- Sanctioned blanket grant: Liquibase creates no sequences (bookkeeping is
-- tables only), so ON ALL SEQUENCES cannot leak DML onto bookkeeping. A future
-- SERIAL table must repeat this in its own grants-<db>-<seq> changeset, since
-- ON ALL SEQUENCES only covers sequences that exist at execution time.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO master_db_app;
-- Defensive no-op on a clean-slate DB: only fixes pre-existing broad grants.
-- It fires once and cannot guard future blanket grants. The permanent guard
-- is the Phase 8 drift check asserting master_db_app has no grants on bookkeeping.
REVOKE ALL PRIVILEGES ON TABLE public.DATABASECHANGELOG, public.DATABASECHANGELOGLOCK FROM master_db_app;
--rollback REVOKE SELECT, INSERT, UPDATE, DELETE ON location, events, events_tag_names, events_tags, events_tag_aliases, events_tag_groups, events_sleep_descriptors, daily, thought_journal, thought_journal_tag_names, thought_journal_tags, thought_journal_tag_aliases, thought_journal_tag_groups, thought_journal_related_item_descriptors FROM master_db_app;
--rollback REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM master_db_app;
-- Rollback of the defensive REVOKE is a deliberate NO-OP (same REVOKE), not an
-- inverse: on a clean-slate DB the forward REVOKE changed nothing, so its
-- "inverse" must not GRANT bookkeeping rights back (that would recreate the
-- Risk-5 exposure). Any pre-existing broad grants it removed on a NON-clean-slate
-- DB are not restored -- acceptable (best-effort).
--rollback REVOKE ALL PRIVILEGES ON TABLE public.DATABASECHANGELOG, public.DATABASECHANGELOGLOCK FROM master_db_app;
