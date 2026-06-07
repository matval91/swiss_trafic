from pathlib import Path
import os
import psycopg2

SQL_FILE = Path(__file__).parent / "init" / "01_schema.sql"

def main():
    database_url = os.environ["DATABASE_URL"]
    sql = SQL_FILE.read_text(encoding="utf-8")

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cur.execute(sql)
        conn.commit()

    print("Schema initialized.")

if __name__ == "__main__":
    main()
