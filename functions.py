import json


def json_loads(s: str):
    """
    Функция преобразует ключевые слова из json в строку
    :param s:
    :return:
    """
    print(json.loads(s))
    return ' '.join(json.loads(s))
