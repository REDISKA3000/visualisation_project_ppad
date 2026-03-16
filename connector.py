import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

pg_creds = {
    'host':     os.getenv('PG_HOST'),
    'port':     os.getenv('PG_PORT'),
    'database': os.getenv('PG_DATABASE'),
    'user':     os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
}

def sql_query(query: str, commit: bool = False, fetch: bool = False,
              insert_data: list = None, batch_size: int = 500000):

    with psycopg2.connect(**pg_creds) as conn:
        cursor = conn.cursor()
        try:
            if insert_data is None:
                cursor.execute(query)
                if commit:
                    conn.commit()
                if fetch:
                    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                    return df
            else:
                for i in range(0, len(insert_data), batch_size):
                    batch = insert_data[i:i + batch_size]
                    cursor.executemany(query, batch)
                    conn.commit()
                    print(f"Батч с {i} по {i + len(batch) - 1} успешно вставлен")
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
        finally:
            cursor.close()