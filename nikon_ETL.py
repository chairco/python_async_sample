# -*- coding:utf-8 -*-
import subprocess as sp
import os
import time
import logging
import uuid
import asyncio

import lazy_logger

from dbs import nikon

from contextlib import contextmanager
from collections import OrderedDict, namedtuple
from itertools import dropwhile, chain
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


ParsedCompletedCommand = namedtuple(
    'ParsedCompletedCommand',
    ['returncode', 'args', 'stdout', 'stderr']
)


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


def decode_cmd_out(completed_cmd):
    try:
        stdout = completed_cmd.stdout.encode('utf-8').decode()
    except AttributeError:
        stdout = str(bytes(completed_cmd.stdout), 'big5').strip()
    try:
        stderr = completed_cmd.stderr.encode('utf-8').decode()
    except AttributeError:
        stderr = str(bytes(completed_cmd.stderr), 'big5').strip()
    return ParsedCompletedCommand(
        completed_cmd.returncode,
        completed_cmd.args,
        stdout,
        stderr
    )


def run_command_under_r_root(cmd, catched=True):
    RPATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'R')
    with cd(newdir=RPATH):
        if catched:
            process = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
        else:
            process = sp.run(cmd)
        return process


def rscript_rot(r, toolid, update_starttime, update_endtime):
    """
    Execute R script tlcd_niknrot.R and tlcd_niknrotmea.R
    """
    rprocess = OrderedDict()
    commands = OrderedDict([
        (toolid, [
            'RScript', r,
            '-t', toolid,
            '-s', update_starttime,
            '-e', update_endtime
        ]),
    ])
    for cmd_name, cmd in commands.items():
        rprocess[cmd_name] = run_command_under_r_root(cmd)
    return rprocess


def rscript_mea(r, toolid, update_starttime, update_endtime):
    """
    Execute R script tlcd_niknrot.R and tlcd_niknrotmea.R
    """
    rprocess = OrderedDict()
    commands = OrderedDict([
        (toolid, [
            'RScript', r,
            '-s', update_starttime,
            '-e', update_endtime
        ]),
    ])
    for cmd_name, cmd in commands.items():
        rprocess[cmd_name] = run_command_under_r_root(cmd)
    return rprocess


def rscript_avm(r, toolid, starttime, endtime):
    rprocess = OrderedDict()
    commands = OrderedDict([
        (toolid, [RScript, r, starttime, endtime]),
    ])
    for cmd_name, cmd in commands.items():
        rprocess[cmd_name] = run_command_under_r_root(cmd)
    return rprocess


class DBbase:
    """docstring for DB
    """
    def __init__(self):
        super(DBbase, self).__init__()
        self.fdc_psql = nikon.FdcPGSQL()
        self.fdc_oracle = nikon.FdcOracle()
        self.eda_oracle = nikon.EdaOracle()


    @classmethod
    def get_aplastendtime(self, apname):
        row = self.fdc_psql.get_lastendtime(
            toolid=self.toolid,
            apname=apname
        )
        return row


class ETL(DBbase):
    """docstring for ETL
    """

    def __init__(self, toolid):
        super(ETL, self).__init__()
        self.toolid = toolid

    def column_state(self, edc, schema):
        add_cols = list(set(edc) - set(schema))
        del_cols = list(set(schema) - set(edc))

        if add_cols and del_cols:
            return {'ret': False, 'add': add_cols, 'del': del_cols}
        elif add_cols:
            return {'ret': False, 'add': add_cols, 'del': del_cols}
        elif del_cols:
            return {'ret': False, 'add': add_cols, 'del': del_cols}
        else:
            return {'ret': True, 'add': add_cols, 'del': del_cols}

    @asyncio.coroutine
    def insert(self, toolid):
        while True:
            row = yield
            if row is None:
                break
            #print('Insert: {}'.format(row))
            self.fdc_psql.save_edcdata(
                toolid=toolid,
                edcdata=row
            )

    @asyncio.coroutine
    def grouper(self, toolid):
        while True:
            yield from self.insert(toolid=toolid)

    @logger.patch
    def etl(self, apname, *args, **kwargs):
        """start etl edc import
        """
        print('Nikon ETL Process Start...')
        row = self.get_aplastendtime(apname=apname)
        etlflow = ckflow(row=row)

        if etlflow:
            ora_lastendtime = self.fdc_oracle.get_lastendtime()[0]
            psql_lastendtime = get_lastendtime(row=row)
            print('Lastendtime, Oracle:{}, PSQL:{}'.format(
                ora_lastendtime, psql_lastendtime))

        # ora lastendtime new than psql lastendtime.
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

            if len(endtime_data):
                # Add login time in all row.
                insert_data = self.clean_endtimedata(endtime_data=endtime_data)

                try:
                    print('Save clean data in index_glassout')
                    # self.fdc_psql.save_endtime(
                    #    endtime_data=insert_data
                    # )
                except Exception as e:
                    raise e

            # Import data in table
            toolids = list(set(data['TOOLID'].lower()
                               for data in endtime_data))
            print(toolids)
            # insert for loop
            try:
                self.etl_flow(
                    toolids=toolids,
                    psql_lastendtime=psql_lastendtime,
                    ora_lastendtime=ora_lastendtime
                )
            except Exception as e:
                raise e

    @classmethod
    def clean_endtimedata(self, endtime_data):
        insert_data = []
        logintime = datetime.now()
        for d in endtime_data:
            d.setdefault('LOGIN_TIME', logintime)
            insert_data.append(tuple(d.values()))
        return insert_data

    def etl_flow(self, toolids, psql_lastendtime, ora_lastendtime):
        for toolid in sorted(toolids):
            # check table exists or not.
            pgclass = self.fdc_psql.get_pgclass(toolid=toolid)
            print('Toolid: {}, pg_class count: {}'.format(toolid, pgclass))

            if pgclass[0]['count']:
                print('Reday to Import EDC toolid: {}'.format(toolid))
                try:
                    print('Delete rows duplicate...')
                    self.fdc_psql.delete_toolid(
                        toolid=toolid,
                        psql_lastendtime=psql_lastendtime,
                        ora_lastendtime=ora_lastendtime
                    )
                except Exception as e:
                    raise e

                schemacolnames = self.fdc_psql.get_schemacolnames(
                    toolid=toolid
                )
                schemacolnames = self.clean_schemacolnames(
                    schemacolnames=schemacolnames
                )

                edc_data = self.fdc_oracle.get_edcdata(
                    toolid=toolid,
                    psql_lastendtime=psql_lastendtime,
                    ora_lastendtime=ora_lastendtime
                )
                print('Total Count: {}'.format(len(edc_data)))
                datas = self.clean_edcdata(
                    edc_data=edc_data,
                    schemacolnames=schemacolnames
                )

                if len(datas) != 0:
                    try:
                        # Using coroutine to add high performance.
                        self.insert_main(toolid=toolid, datas=datas)
                    except Exception as e:
                        raise e

            # Update last endtime.
            try:
                pass
                # self.fdc_psql.update_lastendtime(
                #    toolid=self.toolid,
                #    apname=apname,
                #    last_endtime=ora_lastendtime
                # )
            except Exception as e:
                raise e

    def insert_main(self, toolid, datas):
        for idx, values in enumerate(datas):
            group = self.grouper(toolid=toolid)
            next(group)
            group.send(values)
        group.send(None)

    @logger.patch
    def rot(self, apname, *args, **kwargs):
        """start etl rot, clean data in psql
        """
        print("Nikon ETL ROT Transform Process Start...")
        row = self.get_aplastendtime(apname=apname)
        edcrow = self.get_aplastendtime(apname='EDC_Import')
        rotflow = ckflow(row=row)

        if rotflow:
            psql_lastendtime_rot = get_lastendtime(row=row)
            psql_lastendtime_edc = get_lastendtime(row=edcrow)
            update_starttime = datetime.strptime(
                '2017-07-13 20:00:27', '%Y-%m-%d %H:%M:%S')
            #update_starttime = psql_lastendtime_rot
            update_endtime = psql_lastendtime_edc
            print('EDC Import Lastendtime: {}, '
                  'ROT Transform Lastendtime: {}'.format(
                      psql_lastendtime_edc, psql_lastendtime_rot
                  ))
        count = 0
        while True:
            # stop if update_starttime same.
            if update_starttime == psql_lastendtime_edc:
                print('Done')
                break

            # TODO short term to break, should remark in product.
            if count == 2:
                print('Exit while loop, execute more then {} times.'.format(count))
                break

            if (update_starttime + timedelta(seconds=86400)) < psql_lastendtime_edc:
                update_endtime = update_starttime + timedelta(seconds=86400)
            # else:
            #    update_endtime = psql_lastendtime_edc

            # Get candidates of toolist
            toolist = self.fdc_psql.get_toolid(
                update_starttime=update_starttime,
                update_endtime=update_endtime
            )
            toolids = list(chain.from_iterable(toolist))
            print(toolids)
            # ROT for loop
            try:
                update_starttime = self.rot_flow(
                    toolids=toolids,
                    update_starttime=update_starttime,
                    update_endtime=update_endtime
                )
            except Exception as e:
                raise e
            count += 1

    def rot_flow(self, toolids, update_starttime, update_endtime):
        # ROT Transform
        for toolid in sorted([id.lower() for id in toolids]):
            print('Candidate {} time period '
                  'start: {}, end: {}.'.format(
                      toolid, update_starttime, update_endtime
                  ))
            # ROT
            nikonrot_data = self.fdc_psql.get_nikonrot(
                toolid=toolid,
                update_starttime=update_starttime,
                update_endtime=update_endtime
            )
            print('ROT Candidate count: {}'.format(
                len(nikonrot_data)
            ))
            if len(nikonrot_data):
                rotlog = self.execute_r_rot(
                    toolid=toolid,
                    update_starttime=update_starttime,
                    update_endtime=update_endtime
                )
            # ROT Mea
            measrot_data = self.eda_oracle.get_measrotdata(
                update_starttime=update_starttime,
                update_endtime=update_endtime
            )
            print('ROT Transform start Meas Candidate count {}'.format(
                len(measrot_data)
            ))
            if len(measrot_data):
                rotmeslog = self.execute_r_rotmea(
                    toolid=toolid,
                    update_starttime=update_starttime,
                    update_endtime=update_endtime
                )

            # TODO which sql command call to data integration??
            try:
                print('Refresh MTV (tlcd_nikon_mea_process_summary_mv) in the end"')
                pass
                # self.fdc_psql.refresh_nikonmea()
            except Exception as e:
                raise e

            # Update lastendtime for ROT_Transform and return
            try:
                print('Update lastendtime for ROT_Transform')
                pass
                # self.fdc_psql.update_lastendtime(
                #    toolid=toolid,
                #    apname=apname,
                #    last_endtime=update_endtime
                #)
                #update_starttime = update_endtime
            except Exception as e:
                raise e
        return update_starttime

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
            # endtime = lastendtime_rot

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

            # TODO ????
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

    @classmethod
    def execute_r_rot(self, toolid, update_starttime, update_endtime):
        # run rscript
        print('{0} ROT Start {0}'.format("**" * 3))
        ret = rscript_rot(
            r='rot.R',
            toolid=toolid,
            update_starttime=update_starttime.strftime('%Y-%m-%d %H:%M:%S'),
            update_endtime=update_endtime.strftime('%Y-%m-%d %H:%M:%S')
        )
        msg = decode_cmd_out(ret[toolid])
        print('args: {}, stdout: {}'.format(msg.args, msg.stdout.replace('\r', '')))
        print('return code: {}, stderr: {}'.format(msg.returncode, msg.stderr))
        print('{0} ROT End {0}'.format("**" * 3))
        return msg

    @classmethod
    def execute_r_rotmea(self, toolid, update_starttime, update_endtime):
        # run rscript
        print('{0} ROT Mea Start {0}'.format("**" * 3))
        ret = rscript_mea(
            r='mea.R',
            toolid=toolid,
            update_starttime=update_starttime.strftime('%Y-%m-%d %H:%M:%S'),
            update_endtime=update_endtime.strftime('%Y-%m-%d %H:%M:%S')
        )
        msg = decode_cmd_out(ret[toolid])
        print('args: {}, stdout: {}'.format(msg.args, msg.stdout.replace('\r', '')))
        print('return code: {}, stderr: {}'.format(msg.returncode, msg.stderr))
        print('{0} ROT Mea End {0}'.format("**" * 3))
        return msg

    @classmethod
    def clean_edcdata(self, edc_data, schemacolnames):
        datas = []
        edc_columns = list(edc_data[0].keys())
        column_state = self.column_state(
            edc=edc_columns, schema=schemacolnames)
        if column_state.get('ret', False):
            print('Column status: ret={} add={} del={}'.format(
                column_state.get('ret'), column_state.get('add'),
                column_state.get('del')
            ))
            datas = [tuple(d.values()) for d in edc_data]
        print('Insert count: {}'.format(len(datas)))
        return datas

    @classmethod
    def clean_schemacolnames(self, schemacolnames):
        return [column[0].upper()
                for column in schemacolnames]


@call_lazylog
def etlmain(*args, **kwargs):
    etl = ETL(toolid='NIKON')
    etl.etl(apname='EDC_Import')
    etl.rot(apname='ROT_Transform')
    # etl.avm(apname='AVM_Process')


if __name__ == '__main__':
    etlmain()
