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
df_list = tabula.read_pdf('data.pdf', pages='all',stream=True)
print(f"Успешно извлечено таблиц-{len(df_list)}")

# Запись DataFrame в базу данных
df_list[0].rename(columns={'Unnamed: 0': 'name'}).to_sql(name='mytable', con=engine, if_exists='append', index=False)

# Запрос данных из таблицы
df = pd.read_sql('SELECT * FROM mytable', con=engine)

# Вывод данных в консоль
print(df)

# Закрытие подключения к базе данных
engine.dispose()
