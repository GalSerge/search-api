import re
import queries
import functions as F


def parse_fields(fields: list):
    """
    Анализирует поля таблицы на наличие указаний
    на использование доп. функций при обработке
    :param fields: список полей
    :return: два списка: найденные функции и очищенные поля
    """
    functions = []
    clean_fields = []

    for field in fields:
        # извлекаем из строки блок с перечислением функций и удаляем его
        funcs_str = re.search(r':{(.+)}', field)
        clean_fields.append(re.sub(r':{(.+)}', '', field))

        # выделяем сами функции
        if funcs_str:
            funcs_list = re.split(r',\s*', funcs_str.group(1))
            functions.append(funcs_list)
        else:
            functions.append(None)

    return functions, clean_fields


def parse_query(query: str):
    """
    Выделяет из запроса поля, определяя их тип (текст или заголовок) и необходимость
    использования доп. функций
    :param query:
    :return:
    """
    patterns = [r'title:{{(.+?)}}', r'content:{{(.+?)}}', r'optional:{{(.+?)}}']
    functions = []
    mask = []

    for i, pattern in enumerate(patterns):
        # ищем группу полей, если есть, выделяем поля
        result = re.search(pattern, query)
        if result:
            fields_group = result.group(1)
        else:
            continue
        fields_group = re.split(r',\s*', fields_group)

        # найденные поля анализируем на наличие доп. функций
        funcs, fields = parse_fields(fields_group)

        # собираем найденные функции, очищаем запрос, составляем маску для полей
        functions += funcs
        query = re.sub(pattern, ', '.join(fields), query)
        mask += [i] * len(fields)

    return query, functions, mask


def prepare_fields(config: dict):
    """
    Готовит поля из конфига к формированию запроса и последующей обработке результата
    :param config:
    :return:
    """
    fields_types = ['title', 'content', 'optional']
    fields = dict()
    functions = []
    mask = []

    for i, type_ in enumerate(fields_types):
        funcs, clean_fields = parse_fields(config['fields_' + type_])
        fields[type_] = '' if len(clean_fields) == 0 else ', ' + ', '.join(clean_fields)
        functions += funcs
        mask += [i] * len(clean_fields)

    return fields, functions, mask


def get_select_query(config: dict, timestamp: bool = False, every_day: str = ''):
    """
    Формирует запрос на выборку данных для индекса
    :param config:
    :param timestamp:
    :param every_day:
    :return:
    """
    where_cond = ''

    if config.get('queries'):
        query, functions, mask = parse_query(config['queries']['select'])
    else:
        fields, functions, mask = prepare_fields(config)

        query = queries.select.format(
            **config,
            **fields)

    if timestamp and every_day != '' and config['field_timestamp'] != '':
        where_cond = queries.where_timestamp.format(
            field_timestamp=config['field_timestamp'],
            every_day=every_day
        )

    if config['field_active'] != '':
        active_cond = queries.where_active.format(**config)

        if where_cond == '':
            where_cond = active_cond
        else:
            where_cond += ' AND ' + active_cond

    if where_cond != '':
        query += ' WHERE ' + where_cond
    query += queries.limit

    return query, functions, mask


def get_select_task_query(config: dict, task_table):
    """
    Формирует запрос на извлечение данных из таблицы задания
    :param config:
    :return:
    """
    fields, functions, mask = prepare_fields(config)

    query = queries.select_task.format(**config, **fields, task_table=task_table)
    query += queries.limit

    return query, functions, mask


def prepare_builder_input(docs: list, functions: list, mask: list, config: dict, from_task: bool):
    """
    Обрабатывает результат, полученный из БД, готовит к подаче индексирующему модулю
    :param docs:
    :param functions:
    :param mask:
    :param config:
    :param from_task:
    :return:
    """
    result = []

    for doc in docs:
        doc = list(doc)
        prepared_doc = []
        prepared_doc += doc[:3]

        # указание для индексирующего модуля и коэффициент релевантности
        if from_task:
            prepared_doc += doc[3:5]
        elif doc[3] == config['active_value']:
            prepared_doc += ['add', 0]
            doc.insert(4, '')
        else:
            prepared_doc += ['del']
            result.append(prepared_doc)
            continue

        title_content_optional = ['', '']

        # маска указывает на тип поля: заголовок, контент или доп. поле
        for i in range(len(mask)):
            text = doc[5 + i]
            if functions[i] is not None:
                for f in functions[i]:
                    text = eval(f'F.{f}(text)')

            if mask[i] < 2:
                title_content_optional[mask[i]] += ' ' + text
            else:
                title_content_optional += [text]

        prepared_doc += title_content_optional
        result.append(prepared_doc)

    return result


def get_query(config: dict, task_table: str, every_day: str):
    if config['from_task']:
        select, functions, mask = get_select_task_query(config, task_table)
    else:
        select, functions, mask = get_select_query(config, False, every_day)

    query = select.format(
        batch_size=10,
        start=0)

    return query
