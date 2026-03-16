import psycopg2
import pandas as pd

pg_creds = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'user': 'myuser',
    'password': 'mypassword',
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

### Выгрузка данных из таблицы в DataFrame:
df = sql_query(
    query="SELECT * FROM orders WHERE created_at >= '2024-01-01'",
    fetch=True
)

### Вставка данных из DataFrame в таблицу:

df = pd.DataFrame({
    'user_id': [1, 2, 3],
    'amount':  [100.0, 200.5, 300.0],
    'status':  ['paid', 'pending', 'paid'],
})

insert_query = """
    INSERT INTO orders (user_id, amount, status)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING
"""

sql_query(
    query=insert_query,
    insert_data=df[['user_id', 'amount', 'status']].values.tolist(),
    batch_size=1000
)