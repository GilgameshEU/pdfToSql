import pandas as pd
import tabula
from sqlalchemy import create_engine

import PyPDF2

# Создание подключения к базе данных
db_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'GILGAMESHPC',
    'database': 'localhostDB',
    'Trusted_Connection': 'yes',
    'TrustServerCertificate':'yes'
}

conn_str = ';'.join([f"{key}={value}" for key, value in db_config.items()])
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

df_list = tabula.read_pdf('test_pdf/test.pdf', pages='all',stream=True)
print(f"Успешно извлечено таблиц-{len(df_list)}")

# Объединение всех датафреймов из списка в один датафрейм
df = pd.concat(df_list, ignore_index=True)

# Заполнение всех пропущенных значений в первом столбце нулями
df.iloc[:, 0] = df.iloc[:, 0].fillna("null")

# Выбор строк, содержащих цифры в первом столбце
contains_digits = df[df.iloc[:, 0].astype(str).str.contains('\d+')]
digit_indexes = contains_digits.index.values

indexes_to_drop = list(range(10)) + list(range(48, 58))
df.drop(indexes_to_drop, inplace=True)

df = df.reset_index(drop=True)
contains_digits = df[df.iloc[:, 0].astype(str).str.contains('\d+')]
digit_indexes = contains_digits.index.values

df = df.rename(columns={
    'Unnamed: 0': 'КодТН ВЭД',
    'Unnamed: 1': 'Наименование позиции',
    'Unnamed: 2': 'Доп. ед. изм.',
    'Ставка ввозной': 'Ставка ввозной таможенной пошлины (в процентах от таможенной стоимости либо в евро, либо в долларах США)'
})

# Объединение строк
for i in range(len(digit_indexes)):
    start_index = digit_indexes[i]
    if i == len(digit_indexes) - 1:
        end_index = len(df)
    else:
        end_index = digit_indexes[i + 1]
    if df.iloc[start_index, 0] != "null":
        df.iloc[start_index, 1] = ' '.join(df.iloc[start_index:end_index, 1].astype(str))
        df.iloc[start_index, 1] = df.iloc[start_index, 1].replace('\n', ' ')

# Удаление строк со значением "null" в первом столбце
df = df[df.iloc[:, 0] != "null"]

# Создание словаря с главными категориями
category_dict = {
    '9701': 'Картины, рисунки и пастели, выполненные полностью от руки, кроме рисунков, указанных в товарной позиции 4906, и прочих готовых изделий, разрисованных или декорированных от руки; коллажи, мозаики и аналогичные декоративные изображения: – возрастом более 100 лет:',
    '9702': 'Подлинники гравюр, эстампов и литографий:',
    '9703': 'Подлинники скульптур и статуэток из любых материалов:',
    '9705': 'Коллекции и предметы коллекционирования по археологии, этнографии, истории, зоологии, ботанике, минералогии, анатомии, палеонтологии или нумизматике:',
    '9706': 'Антиквариат возрастом более 100 лет:',
}

# Пройти по всем строкам датафрейма и добавить главные категории
for i, row in df.iterrows():
    code = str(row['КодТН ВЭД'])
    if len(code) > 4 and code[:4] in category_dict:
        category = category_dict[code[:4]]
        df.at[i, 'Наименование позиции'] = category + ' ' + row['Наименование позиции']

df['Доп. ед. изм.'] = df['Доп. ед. изм.'].replace('–', 'шт')

# Сохранение объединенного датафрейма в CSV
df.to_csv("output_combined.csv", index=False, encoding='utf-8')

df.to_sql(name='table_name', con=engine, if_exists='replace', index=False)

engine.dispose()
