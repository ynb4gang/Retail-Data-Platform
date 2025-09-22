# ./superset_home/superset_config.py

import os

# Генерация безопасного ключа (можно заменить своим)
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'RUIAkzIBbx8Zs9acfn9VGoko0K+li5DvaHzVQY6mpCw=')
