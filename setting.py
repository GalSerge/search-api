import json
import string
import secrets
import os
from sqlalchemy import create_engine

with open('config.json', 'r') as f:
    configs = json.load(f)

for i, site in enumerate(configs):
    # генерация ключа для соединения с приложением
    if site['APP_KEY'] == '':
        configs[i]['APP_KEY'] = ''.join(secrets.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(40))

    # создание директории для индекса
    path = 'index/' + site['APP']
    if not os.path.exists(path):
        os.mkdir(path)
        os.mkdir(path + '/titles')
        os.mkdir(path+'/texts')

    # проверка соединения с базой
    engine = create_engine('{driver}://{username}:{password}@{host}:{port}/{database}'.format(**site))
    connection = engine.connect()
    engine.dispose()


with open('config.json', 'w') as f:
    f.write(json.dumps(configs, indent=2))
