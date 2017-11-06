import os
import time
import logging
import uuid
import nikon
import lazy_logger

from datetime import datetime

logger = logging.getLogger(__name__)


def log_time():
    """
    rtype: str time
    """
    return str(time.strftime("%Y%m%d%H%M", time.localtime(time.time())))


def call_lazylog(f):
    def lazylog(*args, **kwargs):
        log_path = os.path.join(os.getcwd(), 'logs',
                                log_time() + '-' + str(uuid.uuid1()) + '.log')
        lazy_logger.log_to_console(logger)
        lazy_logger.log_to_rotated_file(logger=logger, file_name=log_path)
        logger.info('logger file: {0}'.format(log_path))
        kwargs['log_path'] = log_path
        return f(*args, **kwargs)
    return lazylog


@logger.patch
def get_lastendtime(apname, rows):
    """get lastendtime from rows
    :types: apname: str
    :types: rows: list(dict())
    :rtype: dict()
    """
    for row in rows:
        if row['apname'] = apname:
            return row['last_end_time']
    return


@logger.patch
def ckflow(apname, rows):
    """check etl flow
    :types: apname: str
    :types: rows: list(dict())
    :rtype: bool()
    """
    if len(rows):
        for row in rows:
            if row['apname'] == apname
                return True
    return False


@logger.patch
def etl(toolid, apname, start_time=datetime.now(), **kwargs):
    """start etl, get the ap's lasttime of ETL
    :types: toolid: str
    """
    fdc_psql = nikon.FdcPGSQL()
    rows = fdc_psql.get_lastendtime(toolid=toolid)
    etlflow = ckflow(apname=apname, rows=rows)
    
    if etlflow:
        fdc_oracle = nikon.FdcOracle()
        ora_lastendtime = fdc_oracle.get_lastendtime()
        psql_lastendtime = get_lastendtime(apname=apnam, rows=rows)
    


if __name__ == '__main__':
    logger = lazy_logger.get_logger()
    lazy_logger.log_to_console(logger)
    etl(toolid='NIKON', apname='FDC_Import')


