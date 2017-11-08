import subprocess as sp
import os
import time
import logging
import uuid

import lazy_logger

from dbs import nikon

from contextlib import contextmanager
from collections import OrderedDict
from itertools import dropwhile, chain
from datetime import datetime, timedelta

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


def get_lastendtime(row):
    """get lastendtime from row, get the first return.
    :types: rows: list(dict())
    :rtype: datatime
    """
    row = row[0]  # get first row
    return row['last_end_time']


def ckflow(row):
    """check etl flow, if exist more than 1 row return True
    :types: rows: list(dict())
    :rtype: bool()
    """
    if len(row):
        return True
    return False


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(newdir)
    try:
        yield
    finally:
        os.chdir(prevdir)


def run_command_under_r_root(cmd, catched=True):
    RPATH = os.path.join(os.path.abspath(__file__), 'R')
    with cd(newdir=RPATH):
        if catched:
            process = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        else:
            process = sp.run(cmd)
        return process


def rscript(r, toolid, df):
    rprocess = OrderedDict()
    commands = OrderedDict([
        (toolid, [RSript, r, df]),
    ])
    for cmd_name, cmd in commands.items()
        rprocess[cmd_name] = run_command_under_r_root(cmd)
    return rprocess


def rscript_avm(r, toolid, starttime, endtime):
    rprocess = OrderedDict()
    commands = OrderedDict([
        (toolid, [RSript, r, starttime, endtime]),
    ])
    for cmd_name, cmd in commands.items()
        rprocess[cmd_name] = run_command_under_r_root(cmd)
    return rprocess


class ETL:
    """docstring for ETL
    """

    def __init__(self, toolid):
        super(ETL, self).__init__()
        self.fdc_psql = nikon.FdcPGSQL()
        self.fdc_oracle = nikon.FdcOracle()
        self.eda_oracle = nikon.EdcOracle()
        self.toolid = toolid

    @logger.patch
    def etl(self, apname, *args, **kwargs):
        """start etl edc import
        """
        print("Nikon ETL Process Start...")
        row = self.get_aplastendtime(apname=apname)
        etlflow = ckflow(row=row)

        if etlflow:
            # TODO transfer oracle lastendtime
            #ora_lastendtime = datetime.now()
            ora_lastendtime = self.fdc_oracle.get_lastendtime()[0]
            psql_lastendtime = get_lastendtime(row=row)
            print('Lastendtime, Oracle:{}, PSQL:{}'.format(
                ora_lastendtime, psql_lastendtime
            ))

        # ora new than psql
        if ora_lastendtime > psql_lastendtime:
            try:
                self.fdc_psql.delete_tlcd(
                    psql_lastendtime=psql_lastendtime,
                    ora_lastendtime=ora_lastendtime
                )
            except Exception as e:
                raise e

            endtime_data = self.fdc_oracle.get_endtimedata(
                psql_lastendtime=psql_lastendtime,
                ora_lastendtime=ora_lastendtime
            )

            # ????
            if len(endtime_data):
                # replace all time?
                endtime_data['login_time'] = datetime.now()
                try:
                    self.fdc_psql.save_endtime(
                        endtime_data=endtime_data
                    )
                except Exception as e:
                    raise e

            # Import data in table
            toolids = list(set(data['TOOLID'] for data in endtime_data))
            for toolid in toolids:
                # if not exist table save.
                pgclass = self.fdc_psql.get_pgclass(toolid=toolid)
                if not len(pgclass):
                    print('EDC Import {}'.format(toolid))
                    schemacolnames = self.fdc_psql.get_schemacolnames(
                        toolid=toolid
                    )

                    try:
                        self.fdc_psql.delete_toolid(
                            toolid=toolid,
                            psql_lastendtime=psql_lastendtime,
                            ora_lastendtime=ora_lastendtime
                        )
                    except Exception as e:
                        raise e

                    edc_data = self.fdc_oracle.get_edcdata(
                        colname=schemacolnames['column_name'],
                        toolid=toolid,
                        psql_lastendtime=psql_lastendtime,
                        ora_lastendtime=ora_lastendtime
                    )

                    try:
                        print('Insert count: {}'.format(toolid))
                        self.fdc_psql.save_edcdata(
                            toolid=toolid,
                            edc_data=edc_data
                        )
                    except Exception as e:
                        raise e

                # Update lastendtime
                try:
                    self.fdc_psql.update_lastendtime(
                        toolid=self.toolid,
                        apname=apname,
                        last_endtime=ora_lastendtime
                    )
                except Exception as e:
                    raise e

    @logger.patch
    def rot(self, apname, *args, **kwargs):
        """start etl rot
        """
        print("Nikon ETL ROT Transform Process Start...")
        row = self.get_aplastendtime(apname=apname)
        edcrow = self.get_aplastendtime(apname='EDC_Import')
        rotflow = ckflow(row=row)

        if rotflow:
            psql_lastendtime_edc = get_lastendtime(row=edcrow)
            psql_lastendtime_rot = get_lastendtime(row=row)
            update_starttime = psql_lastendtime_rot
            #update_endtime = psql_lastendtime_edc
            print('EDC Import Lastendtime: {}'
                  'ROT Transform Lastendtime: {}'.format(
                      psql_lastendtime_edc, psql_lastendtime_rot
                  ))

        while True:
            # stop on here
            if update_starttime == psql_lastendtime_edc:
                break

            update_starttime += timedelta(seconds=86400)
            if update_starttime < psql_lastendtime_edc:
                update_endtime = update_starttime
            else:
                update_endtime = psql_lastendtime_edc

            # Get candidate of toolist
            toolist = self.fdc_psql.get_toolid(
                update_starttime=update_starttime,
                update_endtime=update_endtime
            )
            toolids = list(chain.from_iterable(toolist))

            for toolid in toolids:
                print('Candidate {} time period: {}'.format(
                    toolid, update_starttime, update_endtime
                ))
                nikonrot_data = self.fdc_psql.get_nikonrot(
                    toolid=toolid,
                    update_starttime=update_starttime,
                    update_endtime=update_endtime
                )
                print('Candidate count: {}'.format(
                    len(nikonrot_data)
                ))

                if len(nikonrot_data):
                    # run rscript
                    ret = rscript(
                        r='TLCD_Nikon_ROT.R',
                        toolid=toolid,
                        df=nikonrot_data
                    )
                    print('ROT End...')

                measrot_data = self.eda_oracle.get_measrotdata(
                    update_starttime=update_starttime,
                    update_endtime=update_endtime
                )
                print('ROT Transform start Meas Candidate count {}'.format(
                    len(measrot_data)
                ))

                if len(measrot_data):
                    # run rscript
                    ret = rscript(
                        r='TLCD_NIKON_MEA_ROT.R',
                        toolid=toolid,
                        df=measrot_data
                    )
                    print('ROT Meas End...')

                # TODO which sql command call to data integration??
                print('Refresh MTV (tlcd_nikon_mea_process_summary_mv) in the end..."')
                try:
                    self.fdc_psql.refresh_nikonmea()
                except Exception as e:
                    raise e

                # Update lastendtime for ROT_Transform
                try:
                    self.fdc_psql.update_lastendtime(
                        toolid=toolid,
                        apname=apname,
                        last_endtime=update_endtime
                    )
                    update_starttime = update_endtime
                except Exception as e:
                    raise e

    @logger.patch
    def avm(self, apname, *args, **kwargs):
        """start etl avm
        """
        row_rot = self.get_aplastendtime(apname='ROT_Transform')
        row_avm = self.get_aplastendtime(apname=apname)

        lastendtime_rot = get_lastendtime(row=row_rot)
        lastendtime_avm = get_lastendtime(row=row_avm)

        if lastendtime_rot > lastendtime_avm:
            starttime = lastendtime_avm
            #endtime = lastendtime_rot

        while True:
            if starttime >= lastendtime_rot:
                break

            starttime += timedelta(seconds=86400)
            if starttime < lastendtime_rot:
                endtime = starttime
            else:
                endtime = lastendtime_rot

            # run rscript_avm
            ret = rscript_avm(
                r='TLCD_Nikon_VM_Fcn',
                starttime=starttime,
                endtime=endtime
            )

            # ????
            if ret:
                try:
                    # Update lastendtime table
                    self.fdc_psql.update_lastendtime(
                        toolid=self.toolid,
                        apname=apname,
                        last_endtime=endtime
                    )
                except Exception as e:
                    raise e

    @logger.patch
    def get_aplastendtime(self, apname, *args, **kwargs):
        row = self.fdc_psql.get_lastendtime(
            toolid=self.toolid,
            apname=apname
        )
        return row


if __name__ == '__main__':
    logger = lazy_logger.get_logger()
    lazy_logger.log_to_console(logger)

    etl = ETL(toolid='NIKON')
    etl.etl(apname='EDC_Import')
    etl.rot(apname='ROT_Transform')
    etl.avm(apname='AVM_Process')
