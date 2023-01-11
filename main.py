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


@app.get("/private")
async def private(site_id: int = Depends(get_api_key)):
    return "Welcome to the security test!"


configs = []
seachers = []
connections = []


@app.on_event('startup')
async def startup():
    global configs
    global seachers
    global connections

    configs = await get_configs()
    seachers = await get_seachers(configs)
    connections = await get_db_connections(configs)


@app.get('/updconfig')
async def updconfig():
    try:
        await update_configs()
        return {'status': 'ok'}
    except Exception as e:
        return {'status': 'error',
                'detail': str(e)}


@app.get('/search')
async def search(q: str, batch_size: int = 30, batch_i: int = 0, site_id: int = Depends(get_api_key)):
    if seachers[site_id] is not None:
        try:
            results, size, right_q = await get_answer(q, seachers[site_id], configs[site_id], batch_size, batch_i)
        except Exception as e:
            return {'status': 'error',
                    'detail': str(e)}
    else:
        return {'status': 'error',
                'detail': f'seacher for site {configs[site_id]["APP"]} don\'t loaded'}

    out = {'status': 'ok',
           'result': results,
           'full_size': size}

    if right_q != q:
        out['right_q'] = right_q


@app.get('/reloadindex')
async def reload_index(site_id: int = Depends(get_api_key)):
    try:
        seachers[site_id] = Seacher('index/' + configs[site_id]['APP'])
        await seachers[site_id].load('index/' + configs[site_id]['APP'])
    except Exception as e:
        return {'status': 'error',
                'detail': f'{configs[site_id]["APP"]}: {str(e)}'}

    return {'status': 'ok',
            'detail': f'{configs[site_id]["APP"]}: index successful reloaded'}


@app.on_event('shutdown')
async def shutdown():
    for i in range(len(configs)):
        if connections[i]:
            await connections[i].disconnect()
        if seachers[i]:
            await seachers[i].stop()


@app.get('/build')
async def build(batch_size: int = 100, act: str = 'add', timestamp: int = 0, tab: int = None,
                site_id: int = Depends(get_api_key)):
    actions = ['all', 'upd', 'add', 'del']

    try:
        builder = get_builder(configs[site_id])
    except Exception as e:
        return {'status': 'error',
                'detail': f'Could\'t load IndexBuilder: {str(e)}'}

    if act not in actions:
        return {'status': 'error',
                'detail': f'act={act} is invalid'}

    if not connections[site_id]:
        return {'status': 'error',
                'detail': f'no db connection'}

    if act == 'all':
        pass
    elif act == 'add':
        if configs[site_id]['TASKS_TABLE'] == '':
            await edit_index(act, builder, configs[site_id], connections[site_id])
        else:
            await edit_index_from_task(act, builder, configs[site_id], connections[site_id])
    elif act == 'upd':
        if configs[site_id]['TASKS_TABLE'] == '':
            await edit_index(act, builder, configs[site_id], connections[site_id])
        else:
            await edit_index_from_task(act, builder, configs[site_id], connections[site_id])
    elif act == 'del':
        if configs[site_id]['TASKS_TABLE'] == '':
            await delete_index(builder, configs[site_id], tab)
        else:
            await delete_index_from_task(builder, configs[site_id], connections[site_id])

    await builder.stop()

    return {'status': 'ok',
            'detail': f'Index successful edit. Call /reloadindex'}


@app.get('/removeindex')
async def remove_index(site_id: int = Depends(get_api_key)):
    shutil.rmtree('index/' + configs[site_id]['APP'], ignore_errors=True)

    # создание директории для индекса
    path = 'index/' + configs[site_id]['APP']
    os.mkdir(path)
    os.mkdir(path + '/titles')
    os.mkdir(path + '/texts')

    seachers[site_id] = None

    return {'status': 'ok',
            'detail': f'{configs[site_id]["APP"]}: index successful remove'}


@app.get('/')
async def root():
    return {'message': 'Hello World'}
