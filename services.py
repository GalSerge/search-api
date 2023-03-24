import json
import string
import secrets
import os
import sys
from asusearch.search import Seacher
from asusearch.index import IndexBuilder

import databases
from tools import *


async def active_config(app_name):
    config_path = 'configs/' + app_name + '.json'
    with open(config_path, 'r') as f:
        config = json.load(f)

    if config['APP_KEY'] == '':
        config['APP_KEY'] = ''.join(
            secrets.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(40))

    # создание директории для индекса
    path = 'index/' + config['APP']
    if not os.path.exists(path):
        os.mkdir(path)
        os.mkdir(path + '/titles')
        os.mkdir(path + '/texts')

    # проверка соединения с базой
    if config['DB_CONNECT']['port'] != '':
        connection_path = '{driver}://{username}:{password}@{host}:{port}/{database}'.format(**config['DB_CONNECT'])
    else:
        connection_path = '{driver}://{username}:{password}@{host}/{database}'.format(**config['DB_CONNECT'])

    connection = databases.Database(connection_path)
    await connection.connect()

    statuses = {}
    for t in config['TABLES']:
        result = []
        query = ''
        error = 'error: '
        try:
            query = get_query(t, config['TASK_TABLE'], config['INDEX_EVERY_DAY'])
            result = await connection.fetch_all(query=query)
        except Exception as e:
            error += str(e)

        if (result is not None and len(result) > 0):
            statuses[t['table_id']] = 'ok'
        else:
            statuses[t['table_id']] = f'{error} "{query}"'

    await connection.disconnect()

    with open(config_path, 'w') as f:
        f.write(json.dumps(config, indent=2))

    return statuses


async def get_configs():
    configs = dict()
    configs_files = [f.name for f in os.scandir('configs')]
    for config in configs_files:
        try:
            with open('configs/' + config, 'r') as f:
                config = json.load(f)
                configs[config['APP_ID']] = config
        except Exception as e:
            print(e)
    return configs


async def get_seachers(configs: list):
    seachers = dict()

    for _, config in configs.items():
        if config['APP_KEY'] == '':
            continue

        seacher = Seacher('index/' + config['APP'])
        try:
            await seacher.load('index/' + config['APP'])
        except FileNotFoundError:
            seacher = None

        seachers[config['APP_ID']] = seacher

    return seachers


def get_builder(config: dict):
    builder = IndexBuilder('index/' + config['APP'])

    return builder


async def get_db_connections(configs: list):
    connections = dict()

    for _, config in configs.items():
        try:
            if config['DB_CONNECT']['port'] != '':
                connection_path = '{driver}://{username}:{password}@{host}:{port}/{database}'.format(
                    **config['DB_CONNECT'])
            else:
                connection_path = '{driver}://{username}:{password}@{host}/{database}'.format(**config['DB_CONNECT'])

            connection = databases.Database(connection_path)
            await connection.connect()
        except Exception as e:
            connection = None

        connections[config['APP_ID']] = connection

    return connections


async def get_answer(q: str, seacher: Seacher, batch_size: int, batch_i: int, table_id: int):
    return await seacher.answer(q, batch_size, batch_i, table_id)


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

        if t['from_task']:
            select, functions, mask = get_select_task_query(t, config['TASK_TABLE'])
        else:
            select, functions, mask = get_select_query(t, timestamp, config['INDEX_EVERY_DAY'])

        batch = 0
        while True:
            query = select.format(
                batch_size=batch_size,
                start=batch * batch_size)

            result = await db_conn.fetch_all(query=query)
            if len(result) == 0:
                break

            result = prepare_builder_input(result, functions, mask, t, bool(t['from_task']))
            await builder.update(result, t['fields_optional'], config['LANGUAGES'])

            batch += 1

    await builder.save(index_path)

    return True


async def delete_index(builder, config, table: int = -1):
    index_path = 'index/' + config['APP']

    await builder.load(index_path)

    await builder.delete_by_type(table)
    await builder.save(index_path)

    return True
