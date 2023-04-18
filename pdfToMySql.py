from sqlalchemy import create_engine
import pandas as pd
import tabula

# Создание подключения к базе данных
db_config = {
    'user': 'root',
    'password': 'Test1',
    'host': 'localhost',
    'port': 3306,
    'database': 'test'
}

engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Чтение таблицы из PDF и преобразование в DataFrame
df_list = tabula.read_pdf('test_pdf/data.pdf', pages='all',stream=True)
print(f"Успешно извлечено таблиц-{len(df_list)}")

# Запись DataFrame в базу данных
for i, df in enumerate(df_list):
    table_name = f"mytable_{i+1}"
    df.rename(columns={'Unnamed: 0': 'name'}).to_sql(name=table_name, con=engine, if_exists='append', index=False)

# Запрос данных из всех таблиц
all_tables = []
for i in range(len(df_list)):
    table_name = f"mytable_{i+1}"
    table_df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
    all_tables.append(table_df)

# Объединение всех таблиц в один DataFrame
df = pd.concat(all_tables, ignore_index=True)

# Вывод данных в консоль
print(df)

# Закрытие подключения к базе данных
engine.dispose()
