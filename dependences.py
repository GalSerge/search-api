# from fastapi.security.api_key import APIKeyHeader, APIKey
# from fastapi import Security, HTTPException
# from services import configs
#
# api_key_header = APIKeyHeader(name='app-key', auto_error=False)
# 
#
# def get_api_key(api_key_header: str = Security(api_key_header)) -> int:
#     """
#     Проверяет ключ приложения из заголовка
#     :param api_key_header:
#     :return: int id сайта, к которому подходит полученный ключ
#     """
#     global configs
#     for i, config in enumerate(configs):
#         if api_key_header == config['APP_KEY']:
#             return i
#     else:
#         raise HTTPException(
#             status_code=403, detail='could not validate credentials'
#         )
#