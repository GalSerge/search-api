from multiprocessing import cpu_count

PREFIX = '/var/www/search.asu.edu.ru/searchapi'
# reload = True
# Socket Path
bind = '192.168.100.139:8172'
debug = True

# preload = True

# Worker Options
workers = cpu_count() + 1
workers = 4
worker_class = 'uvicorn.workers.UvicornWorker'

timeout = 3600

certfile = PREFIX + '/gunicorn/ssl/cert.crt'
keyfile = PREFIX + '/gunicorn/ssl/cert.key'

# Logging Options
loglevel = 'debug'
accesslog = PREFIX + '/log/access_log'
errorlog = PREFIX + '/log/error_log'