import os
import time
import logging
import uuid

import nikon

import lazy_logger

from itertools import dropwhile, chain

from datetime import datetime

logger = logging.getLogger(__name__)


def log_time():
    """return log datetime
    rtype: str time
    """
    return str(time.strftime("%Y%m%d%H%M", time.localtime(time.time())))


def call_lazylog(f):
    def lazylog(*args, **kwargs):
        log_path = os.path.join(
            os.getcwd(), 'logs',
            log_time() + '-' + str(uuid.uuid1()) + '.log'
        )
        lazy_logger.log_to_console(logger)
        lazy_logger.log_to_rotated_file(logger=logger, file_name=log_path)
        logger.info('logger file: {0}'.format(log_path))
        kwargs['log_path'] = log_path
        return f(*args, **kwargs)
    return lazylog


def get_row(apname, rows):
    row = list(dropwhile(lambda row: row['apname'] == apname, rows))
    return row


def get_lastendtime(row):
    """get lastendtime from row, get the first return.
    :types: rows: list(dict())
    :rtype: datatime
    """
    row = row[0]  # get first
    return row['last_end_time']


def ckflow(row):
    """check etl flow, if exist more than 1 row return True
    :types: rows: list(dict())
    :rtype: bool()
    """
    if len(row):
        return True
    return False


@logger.patch
def etl(toolid, apname, start_time=datetime.now(), **kwargs):
    """start etl import, get the ap's lasttime of ETL
    :types: toolid: str
    """
    fdc_psql = nikon.FdcPGSQL()
    rows = fdc_psql.get_lastendtime(toolid=toolid)
    row = get_row(apname=apname, rows=rows)
    etlflow = ckflow(row=row)

    if etlflow:
        fdc_oracle = nikon.FdcOracle()
        ora_lastendtime = fdc_oracle.get_lastendtime()
        psql_lastendtime = get_lastendtime(row=row)


@logger.patch
def roi(toolid, apname, start_time=datetime.now(), **kwargs):
    """start etl roi 
    """
    pass


@logger.patch
def avm(toolid, apname):
    """start etl avm
    """
    pass


if __name__ == '__main__':
    logger = lazy_logger.get_logger()
    lazy_logger.log_to_console(logger)
    etl(toolid='NIKON', apname='FDC_Import')
