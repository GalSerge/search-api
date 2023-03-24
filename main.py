# -*- coding: utf-8 -*-

import databases
import os
import shutil
import time
import uvicorn

from fastapi import FastAPI, Depends, HTTPException, Security
# from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.security.api_key import APIKeyHeader, APIKey

from services import *

api_key_header = APIKeyHeader(name='APP-KEY', auto_error=True)


def get_api_key(api_key_header: str = Security(api_key_header)) -> int:
    """
    Проверяет ключ приложения из заголовка
    :param api_key_header:
    :return: id сайта, к которому подходит полученный ключ
    """
    global configs
    for i, config in enumerate(configs):
        if api_key_header == config['APP_KEY']:
            return i
    else:
        raise HTTPException(
            status_code=403, detail='could not validate credentials'
        )


app = FastAPI(debug=False)

origins = [
    "http://asu.edu.ru",
    "https://asu.edu.ru",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.add_middleware(HTTPSRedirectMiddleware)


configs = []
seachers = []
connections = []


@app.on_event('startup')
async def startup():
    global configs
    global seachers
    global connections

    configs = await get_configs()
    if len(configs) > 0:
        print(f'configs ({len(configs)}) uploaded successfully')
    seachers = await get_seachers(configs)
    if len(seachers) > 0:
        print(f'seachers ({len(configs)}) uploaded successfully')
    connections = await get_db_connections(configs)
    if len(connections) > 0:
        print(f'connections ({len(connections)}) completed successfully')


@app.get('/updconfig')
async def updconfig(app_name: str):
    try:
        statuses = await active_config(app_name)
        detail = 'Queries test results:\n'
        for t, status in statuses.items():
            detail += f'Table {t}: {str(status)} \n'
        return {'status': 'ok', 'detail': detail}
    except Exception as e:
        return {'status': 'error',
                'detail': str(e)}


@app.get('/search')
async def search(q: str, batch_size: int = 30, batch_i: int = 0, type: int = -1, site_id: int = 0):
    if seachers[site_id] is not None:
        results, size, right_q = await get_answer(q, seachers[site_id], batch_size, batch_i, type)
    else:
        return {'status': 'error',
                'detail': f'seacher for site {configs[site_id]["APP"]} don\'t loaded'}

    out = {'status': 'ok', 'result': results, 'full_size': size, 'right_q': right_q}

    return out


@app.on_event('shutdown')
async def shutdown():
    for i in range(len(configs)):
        if connections[i]:
            await connections[i].disconnect()
        if seachers[i]:
            await seachers[i].stop()


@app.get('/build')
async def build(batch_size: int = 100, table_id: int = -1, timestamp: int = 0,
                site_id: int = Depends(get_api_key)):
    try:
        builder = get_builder(configs[site_id])
    except Exception as e:
        return {'status': 'error',
                'detail': f'{configs[site_id]["APP"]}: could\'t load IndexBuilder: {str(e)}'}

    if not connections[site_id]:
        return {'status': 'error',
                'detail': f'{configs[site_id]["APP"]}: no db connection'}

    await edit_index(builder, configs[site_id], connections[site_id], batch_size, timestamp, table_id)
    await builder.stop()

    return {'status': 'ok',
            'detail': f'{configs[site_id]["APP"]}: index successful edit.'}


@app.get('/clear')
async def clear_index(site_id: int = 0):
    shutil.rmtree('index/' + configs[site_id]['APP'], ignore_errors=True)

    # создание директории для индекса
    path = 'index/' + configs[site_id]['APP']
    os.mkdir(path)
    os.mkdir(path + '/titles')
    os.mkdir(path + '/texts')

    seachers[site_id] = None

    return {'status': 'ok',
            'detail': f'{configs[site_id]["APP"]}: index successful cleared'}


@app.get('/delete')
async def delete(type: int = -1, site_id: int = 0):
    try:
        builder = get_builder(configs[site_id])
        await delete_index(builder, configs[site_id], type)
        await builder.stop()
    except Exception as e:
        return {'status': 'error',
                'detail': f'{configs[site_id]["APP"]}: could\'t delete: {str(e)}'}

    return {'status': 'ok',
            'detail': f'{configs[site_id]["APP"]}: index successful delete'}


@app.get('/')
async def root():
    return {'message': 'Hello World'}
