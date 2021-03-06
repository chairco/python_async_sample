import atexit

import cx_Oracle

from .env import DATABASE_INFO_EDA


_arg_key_pairs = [
    ('host', 'HOST'),
    ('port', 'PORT'),
    ('user', 'USER'),
    ('password', 'PASSWORD'),
    ('dbname', 'NAME'),
]


def _build_connct_arg():
    return {
        arg: DATABASE_INFO_EDA[key]
        for arg, key in _arg_key_pairs
        if DATABASE_INFO_EDA[key]
    }


_conn = None


def cleanup():
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def commit():
    if _conn is not None:
        _conn.commit()


def get_cursor():
    global _conn
    if _conn is None:
        arg = _build_connct_arg()
        dns_tns = cx_Oracle.makedsn(arg['host'], arg['port'], arg['dbname'])
        _conn = cx_Oracle.connect(arg['user'], arg['password'], dns_tns, threaded=True)
        atexit.register(cleanup)
    return _conn.cursor()
