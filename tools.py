import re


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
            funcs_list = re.split(r';\s*', funcs_str.group(1))
            functions.append(funcs_list)
        else:
            functions.append(None)

    return functions, clean_fields


def parse_query(query: str):
    """
    Выделяет из запроса поля, определяя их тип (текст или заголовок) и
    использование доп. функций
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
