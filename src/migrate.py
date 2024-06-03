import pyodbc
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Загрузка параметров подключения из файла .env
load_dotenv()

# Параметры подключения к SQL Express
sql_server = os.getenv('SQL_SERVER')
sql_database = os.getenv('SQL_DATABASE')
sql_driver = os.getenv('SQL_DRIVER')

# Параметры подключения к PostgreSQL
pg_host = os.getenv('PG_HOST')
pg_database = os.getenv('PG_DATABASE')
pg_username = os.getenv('PG_USERNAME')
pg_password = os.getenv('PG_PASSWORD')

# Функция для подключения к SQL Express
def connect_to_sql():
    connection_string = (
        f'DRIVER={{{sql_driver}}};'
        f'SERVER={sql_server};'
        f'DATABASE={sql_database};'
        f'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

# Функция для подключения к PostgreSQL
def connect_to_postgres():
    return psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_username,
        password=pg_password
    )

# Функция для получения списка таблиц из базы данных SQL Express
def get_sql_tables(sql_conn):
    query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_catalog = ?"
    return pd.read_sql(query, sql_conn, params=[sql_database])['table_name'].tolist()

# Функция для получения схемы таблицы
def get_table_schema(sql_conn, table_name):
    query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    return pd.read_sql(query, sql_conn)

# Функция для создания таблицы в PostgreSQL
def create_postgres_table(pg_cursor, table_name, schema):
    column_definitions = []
    for _, row in schema.iterrows():
        column_name = row['COLUMN_NAME']
        data_type = row['DATA_TYPE']
        pg_data_type = convert_data_type(data_type)
        column_definitions.append(f"{column_name} {pg_data_type}")
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
    pg_cursor.execute(create_table_query)

# Функция для конвертации типов данных
def convert_data_type(sql_data_type):
    type_mapping = {
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bit': 'BOOLEAN',
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'money': 'MONEY',
        'float': 'FLOAT',
        'real': 'REAL',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'TEXT'
    }
    return type_mapping.get(sql_data_type, 'TEXT')

# Функция для переноса данных таблицы
def migrate_table_data(sql_conn, pg_conn, table_name):
    sql_query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(sql_query, sql_conn)

    pg_cursor = pg_conn.cursor()
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        pg_cursor.execute(insert_query, tuple(row))

    pg_conn.commit()

# Основная функция для миграции базы данных
def migrate_database():
    sql_conn = connect_to_sql()
    pg_conn = connect_to_postgres()
    pg_cursor = pg_conn.cursor()

    tables = get_sql_tables(sql_conn)
    for table in tables:
        schema = get_table_schema(sql_conn, table)
        create_postgres_table(pg_cursor, table, schema)
        migrate_table_data(sql_conn, pg_conn, table)
        print(f"Table {table} successfully migrated.")

    pg_cursor.close()
    pg_conn.close()
    sql_conn.close()

if __name__ == '__main__':
    migrate_database()
    print("Database successfully migrated from SQL Express to PostgreSQL.")
