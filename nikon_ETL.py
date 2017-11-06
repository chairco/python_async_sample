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
def process(apname, rows):
    ret = {}
    if len(rows):
        for row in rows:
            if row['apname'] == apname:
                ret.setdefault('psql', row['last_end_time'])
        #ret.setdefault('ora', fdc_oracle.get_lastendtime())
    return ret

@logger.patch
def etl(toolid, start_time=datetime.now(), **kwargs):
    # start etl, get the ap's lasttime of ETL
    fdc_psql = nikon.FdcPGSQL()
    fdc_oracle = nikon.FdcOracle()
    rows = fdc_psql.get_lastendtime(toolid=toolid)
    ret = process(apname='FDC_Import', rows=rows)
    


if __name__ == '__main__':
    logger = lazy_logger.get_logger()
    lazy_logger.log_to_console(logger)
    etl(toolid='CVDU01')