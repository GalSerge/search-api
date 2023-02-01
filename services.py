import json
import string
import secrets
import os
import sys
from asusearch.search import Seacher
from asusearch.index import IndexBuilder

import databases
from queries import *


async def update_configs():
    with open('config.json', 'r') as f:
        configs = json.load(f)

    for i, site in enumerate(configs):
        # генерация ключа для соединения с приложением
        if site['APP_KEY'] == '':
            configs[i]['APP_KEY'] = ''.join(
                secrets.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(40))

        # создание директории для индекса
        path = 'index/' + site['APP']
        if not os.path.exists(path):
            os.mkdir(path)
            os.mkdir(path + '/titles')
            os.mkdir(path + '/texts')

        # проверка соединения с базой
        # connection = await databases.Database('{driver}://{username}:{password}@{host}:{port}/{database}'
        #                                 .format(**site['DB_CONNECT'])).connect()
        # connection.disconnect()
        # engine = create_engine('{driver}://{username}:{password}@{host}:{port}/{database}'.format(**site['DB_CONNECT']))
        # connection = await engine.connect()
        # engine.dispose()

    with open('config.json', 'w') as f:
        f.write(json.dumps(configs, indent=2))


async def get_configs():
    with open('config.json', 'r') as f:
        configs = json.load(f)
    return configs


async def get_seachers(configs: list):
    seachers = []

    for config in configs:
        if config['APP_KEY'] == '':
            continue

        seacher = Seacher('index/' + config['APP'])
        try:
            await seacher.load('index/' + config['APP'])
        except FileNotFoundError:
            seacher = None

        seachers.append(seacher)

    return seachers


def get_builder(config: dict):
    builder = IndexBuilder('index/' + config['APP'])

    return builder


async def get_db_connections(configs: list):
    connections = []

    for config in configs:
        try:
            connection = databases.Database('{driver}://{username}:{password}@{host}/{database}'
                                            .format(**config['DB_CONNECT']))
            await connection.connect()
        except Exception as e:
            print(e)
            connection = None

        connections.append(connection)

    return connections


async def get_answer(q: str, seacher: Seacher, batch_size: int, batch_i: int, table_id: int):
    distances, docs, size, right_query = await seacher.answer(q, batch_size, batch_i, table_id)
    results = []

    for i in range(len(distances)):
        results.append({
            'score': float(distances[i]),
            'doc': docs[i]
        })

        if docs[i][3] is not None:
            results[i]['optional'] = json.loads(docs[i][3])

    return results, size, right_query


# async def edit_index_from_task(act: str, builder, config, db_conn, batch_size: int = 100):
#     """
#     Добавляет записи в индекс и обновляет их, используя таблицу с заданием на индекс
#     :param act: задание 'upd' или 'add'
#     :param builder: объект класса построителя индекса IndexBuilder
#     :param config: dict структуры индексируемых таблиц
#     :param db_conn: подключение к базе
#     :param batch_size: размер для одного запроса на индекс
#     :return: bool
#     """
#     index_path = 'index/' + config['APP']
#     query = 'SELECT `id` FROM {tasks_table} ORDER BY `id` DESC LIMIT 1'.format(tasks_table=config['TASKS_TABLE'])
#     num_rows = await db_conn.fetch_one(query=query)
#     num_rows = num_rows[0]
#
#     if act == 'upd':
#         await builder.load(index_path)
#
#     for batch in range(0, num_rows, batch_size):
#         for t in config['TABLES']:
#             query = query_select_from_task.format(
#                 **t,
#                 lang=t['table_name'] + '.' + t['field_lang'] if t['field_lang'] != '' else 'NULL',
#                 title=f'CONCAT_WS(\' \', {", ".join(t["fields_title"])})',
#                 content=f'CONCAT_WS(\' \', {", ".join(t["fields_content"])})',
#                 optional=', ' + ', '.join(t['fields_optional']) if len(t['fields_optional']) > 0 else '',
#                 tasks_table=config['TASKS_TABLE'],
#                 min_id=batch,
#                 max_id=batch + batch_size,
#                 act=act
#             )
#
#             result = await db_conn.fetch_all(query=query)
#
#             if result:
#                 if act == 'add':
#                     await builder.add(result, t['fields_optional'], config['LANGUAGES'])
#                 elif act == 'upd':
#                     await builder.update(result, t['fields_optional'], config['LANGUAGES'])
#
#     await builder.save(index_path)
#
#     return True
#

# async def edit_index(act: str, builder, config, db_conn, batch_size: int = 100, timestamp: bool = False):
#     """
#     Добавляет записи в индекс и обновляет его непосредственно из таблиц
#     :param act: задание 'upd' или 'add'
#     :param builder: объект класса построителя индекса IndexBuilder
#     :param config: dict структуры индексируемых таблиц
#     :param db_conn: подключение к базе
#     :param batch_size: размер для одного запроса на индекс
#     :param timestamp: учитывать ли время обновления записей
#     :return: bool
#     """
#     index_path = 'index/' + config['APP']
#
#     if act == 'upd':
#         await builder.load(index_path)
#
#     for t in config['TABLES']:
#         query = query_num_rows.format(table_name=t['table_name'])
#         num_rows = await db_conn.fetch_one(query=query)
#         num_rows = num_rows[0]
#
#         for batch in range(0, num_rows, batch_size):
#             if timestamp and t['field_update'] != '':
#                 query = query_select_on_time.format(
#                     **t,
#                     lang=t['field_lang'] if t['field_lang'] != '' else 'NULL',
#                     title=f'CONCAT_WS(\' \', {", ".join(t["fields_title"])})',
#                     content=f'CONCAT_WS(\' \', {", ".join(t["fields_content"])})',
#                     min_id=batch,
#                     max_id=batch + batch_size,
#                     every=config['INDEX_EVERY_DAY']
#                 )
#             else:
#                 query = query_select.format(
#                     **t,
#                     lang=t['field_lang'] if t['field_lang'] != '' else 'NULL',
#                     title=f'CONCAT_WS(\' \', {", ".join(t["fields_title"])})',
#                     content=f'CONCAT_WS(\' \', {", ".join(t["fields_content"])})',
#                     min_id=batch,
#                     max_id=batch + batch_size,
#                 )
#
#             result = await db_conn.fetch_all(query=query)
#
#             ############ костыль ############
#             # необходим для работы поиска с ключевыми словами
#
#             if config['APP'] == 'kaspy.asu.edu.ru' and t['table_id'] == '2':
#                 for i in range(len(result)):
#                     result[i] = list(result[i])
#                     result[i][3] = ' '.join(json.loads(result[i][3]))
#                     result[i][4] = ' '.join(json.loads(result[i][4]))
#
#             #################################
#
#             if act == 'add':
#                 await builder.add(result, t['fields_optional'], config['LANGUAGES'])
#             elif act == 'upd':
#                 await builder.update(result, t['fields_optional'], config['LANGUAGES'])
#
#     await builder.save(index_path)
#
#     return True


async def edit_index(builder: IndexBuilder, config: dict, db_conn, batch_size: int, timestamp: bool = False,
                     table_id: int = -1):
    """
    Добавляет записи в индекс и обновляет его непосредственно из таблиц
    :param builder: объект класса построителя индекса IndexBuilder
    :param config: dict структуры индексируемых таблиц
    :param db_conn: подключение к базе
    :param batch_size: размер для одного запроса на индекс
    :param timestamp: учитывать ли время обновления записей
    :param table_id: id индексируемой таблицы, если равно -1, индексируются все
    :return: bool
    """
    index_path = 'index/' + config['APP']

    await builder.load(index_path)

    for t in config['TABLES']:
        if table_id != -1 and t['table_id'] != table_id:
            continue

        if t['from_task'] == 1:
            select, functions, mask = get_select_task_query(t)
        else:
            select, functions, mask = get_select_query(t, timestamp, config['INDEX_EVERY_DAY'])

        while True:
            query = select.format(
                batch_size=batch_size,
                start=batch + batch_size)

            result = await db_conn.fetch_all(query=query)

            if len(result) == 0:
                break

            result = prepare_builder_input(result, functions, mask, t, bool(t['from_task']))
            await builder.update(result, t['fields_optional'], config['LANGUAGES'])

    await builder.save(index_path)

    return True


# async def delete_index_from_task(builder, config, db_conn, batch_size: int = 100):
#     index_path = 'index/' + config['APP']
#     query = 'SELECT `id` FROM {tasks_table} ORDER BY `id` DESC LIMIT 1'.format(tasks_table=config['TASKS_TABLE'])
#     num_rows = await db_conn.fetch_one(query=query)
#     num_rows = num_rows[0]
#
#     await builder.load(index_path)
#
#     for batch in range(0, num_rows, batch_size):
#         query = query_select_task.format(
#             tasks_table=config['TASKS_TABLE'],
#             min_id=batch,
#             max_id=batch + batch_size,
#             act='del'
#         )
#
#         result = await db_conn.fetch_all(query=query)
#
#         if result:
#             await builder.delete(result)
#
#     await builder.save(index_path)
#
#     return True


async def delete_index(builder, config, tables: list = []):
    index_path = 'index/' + config['APP']

    builder.load(index_path)

    # если не передан список таблиц на удаление,
    # то очищаются все таблицы
    if not tables or len(tables) == 0:
        tables = config['TABLES']

    for t in enumerate(tables):
        await builder.delete_by_type(t['table_id'])

    await builder.save(index_path)

    return True


async def delete_from_task(act: str, config, db_conn):
    query_delete_task.format(
        act=act,
        tasks_table=config['TASKS_TABLE']
    )

    await db_conn.fetch_all(query=query)
