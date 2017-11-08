import os
import pathlib
import logging

import dj_database_url
import dotenv


logger = logging.getLogger(__name__)

__all__ = ['ROOT_DIR_PATH', 'DATABASE_INFO', 'DATABASE_INFO_PG']


ROOT_DIR_PATH = pathlib.Path(__file__).resolve().parent.parent

dotenv_path = ROOT_DIR_PATH.joinpath('.env')
if dotenv_path.exists():
    dotenv.load_dotenv(str(dotenv_path))


DATABASE_INFO_EDA = dj_database_url.parse(os.environ['DATABASE_URL_EDA'])

DATABASE_INFO_FDC = dj_database_url.parse(os.environ['DATABASE_URL_FDC'])

DATABASE_INFO_PG = dj_database_url.parse(os.environ['DATABASE_URL_PG'])


# This logs everything to stderr.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {    # Match access log style.
            'format': '[%(asctime)s] "%(levelname)s %(name)s" %(message)s',
            'datefmt': r'%d/%b/%Y %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 0,
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'root': {
        'level': 0,
        'handlers': ['console'],
    }
}
