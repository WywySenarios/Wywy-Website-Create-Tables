def main():
    # create the info table
    with psycopg2.connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'sync_status');")
            tableExists = cur.fetchone()[0]
            
            if tableExists:
                print("Table \"sync_status\" already exists in database \"info\"; skipping creation.")
            else:
                cur.execute("""
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

                    CREATE TABLE sync_status (
                        id SERIAL PRIMARY KEY,
                        table_name TEXT NOT NULL,
                        parent_table_name TEXT NOT NULL,
                        table_type TEXT NOT NULL,
                        database_name TEXT NOT NULL,
                        entry_id TEXT NOT NULL,
                        remote_id TEXT NULL,
                        sync_timestamp TIMESTAMPTZ NULL,
                        status sync_status_enum NULL
                    );
                """)