# -*- coding:utf-8 -*-
import os
import time
import multiprocessing
import inspect
import logging
import uuid
import lazy_logger

from functools import wraps
from datetime import datetime
from concurrent import futures
from itertools import chain

import auto

from cktypes import checktypes


logger = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

MAX_WORKER = 200


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


class Querybase:
    """
    Here is query base with concurrency. using high level multiprocess.
    """

    @logger.patch
    def _query_history_concurrency(self, query, glass_id):
        """
        Query oracle db by mutiplethread
        :type query: query object
        :type glass_id: list
        :rtype dict()  
        """
        workers = min(MAX_WORKER, len(glass_id))
        with futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_gid = {executor.submit(
                query, g_id): g_id for g_id in sorted(glass_id)}
            result = {}
            for future in futures.as_completed(future_to_gid):
                g_id = future_to_gid[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (g_id, exc))
                else:
                    print('%r glass_id has %d rows' % (g_id, len(data)))
                result.setdefault(g_id, data)
        return result

    @logger.patch
    def _query_data_concurrency(self, query, datas):
        """
        Query oracle db with chain data by mutipleprocess
        :type query: query object
        :type datas: list
        :rtype dict()  
        """
        workers = min(50, len(datas))
        with futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {
                executor.submit(
                    query, value[0], value[1], value[2]): value[0] + '_' + value[1] for value in datas
            }
            result = {}
            for future in futures.as_completed(future_to_sid):
                g_s_id = future_to_sid[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (g_s_id, exc))
                else:
                    #print('%r step_id has %d rows' % (g_s_id, len(data)))
                    pass
                result.setdefault(g_s_id, data)
        return result

    @logger.patch
    def _query_rawdata_concurrency(self, query, datas):
        """
        Query oracle db by mutipleprocess
        :type query: query object
        :type datas: list
        :rtype dict()  
        """
        workers = min(50, len(datas))
        with futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {
                executor.submit(
                    query, value[0], value[1], value[2], value[3]):
                value[0] + '_' + value[1] + '_' + value[3] for value in datas
            }
            result = {}
            for future in futures.as_completed(future_to_sid):
                g_s_id = future_to_sid[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (g_s_id, exc))
                else:
                    #print('%r step_id has %d rows' % (g_s_id, len(data)))
                    pass
                result.setdefault(g_s_id, data)
        return result

    @logger.patch
    def _query_rawdata_sub_concurrency(self, query, datas):
        """
        Query oracle db by mutipleprocess
        :type query: query object
        :type datas: list
        :rtype dict()  
        """
        workers = min(50, len(datas))
        with futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {
                executor.submit(
                    query, value[0], value[1], value[2]):
                value[0] + '_' + value[1] for value in datas
            }
            result = {}
            for future in futures.as_completed(future_to_sid):
                g_s_id = future_to_sid[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (g_s_id, exc))
                else:
                    #print('%r step_id has %d rows' % (g_s_id, len(data)))
                    pass
                result.setdefault(g_s_id, data)
        return result


class Queryedc(Querybase):
    """
    Here is API for query table from oracle db. 
    :types query: auto.query object
    """

    @checktypes
    def glass_history(self, glass_id: list):
        return self._query_history_concurrency(auto.get_edc_glass_history, glass_id)

    @checktypes
    def glass_data(self, glass_id: list):
        sid_dataset = self.glass_history(glass_id)
        values = list(chain.from_iterable(sid_dataset.values()))
        return self._query_data_concurrency(auto.get_edc_data, values)


class Queryteg(Querybase):
    """
    Here is API for query table from oracle db.
    :types query: auto.query object
    """

    @checktypes
    def glass_history(self, glass_id: list):
        return self._query_history_concurrency(auto.get_teg_glass_history, glass_id)

    @checktypes
    def glass_data(self, glass_id: list):
        sid_dataset = self.glass_history(glass_id)
        values = list(chain.from_iterable(sid_dataset.values()))
        return self._query_data_concurrency(auto.get_teg_data, values)

    @checktypes
    def _glass_param_data(self, glass_id: list):
        sid_dataset = self.glass_history(glass_id)
        values = list(chain.from_iterable(sid_dataset.values()))
        return self._query_data_concurrency(auto.get_teg_param_data, values)

    @checktypes
    def _glass_with_param(self, glass_id: list):
        return self._query_history_concurrency(auto.get_sid_with_param, glass_id)

    @checktypes
    def _bind_parm(self, glass_id: list, subquery=False):
        sid_dataset = self._glass_param_data(glass_id)
        gid_with_param = self._glass_with_param(glass_id)

        query_list = {}
        for key, values in sid_dataset.items():
            gid, sid = key.split('_')
            # value not empty save
            if sid in chain.from_iterable(gid_with_param[gid]) and values:
                if subquery:
                    query_list.setdefault(key, values[0])
                else:
                    query_list.setdefault(key, values)
        return query_list

    @checktypes
    def glass_raw_data(self, glass_id: list, subquery=True):
        """
        SQL command using 'param_name = PARAM_NAME' to sequence get param_name data 
        :types: query_list: list in list
        :value: list
        """
        query_list = self._bind_parm(glass_id, subquery)
        if subquery:
            values = list(query_list.values())
            query = auto.get_teg_result_sub
            return self._query_rawdata_sub_concurrency(query, values)
        else:
            values = list(chain.from_iterable(query_list.values()))
            query = auto.get_teg_result
            return self._query_rawdata_concurrency(query, values)


@call_lazylog
def main(*args, **kwargs):
    with open('sample.csv', 'r') as fp:
        glass_id = fp.readlines()
    glass_id = [i.rstrip() for i in glass_id]

    # test teg
    q = Queryteg()

    print('get rawdata without subquery')
    t0 = time.time()
    ret = q.glass_raw_data(glass_id, subquery=False)
    print(len(list(chain.from_iterable(ret.values()))))
    elapsed = time.time() - t0
    msg = '\n{} glass_id query in {:.2f}s'
    print(msg.format(len(ret), elapsed))
    

    print('get rawdata with subquery')
    t0 = time.time()
    ret2 = q.glass_raw_data(glass_id, subquery=True)
    print(len(list(chain.from_iterable(ret2.values()))))
    elapsed = time.time() - t0
    msg = '{} glass_id query in {:.2f}s'
    print(msg.format(len(ret2), elapsed))

    # test edc
    #t0 = time.time()
    #q = Queryedc()
    #ret = q.glass_history(glass_id)
    #elapsed = time.time() - t0
    #msg = '\n{} glass_id query in {:.2f}s'
    #print(msg.format(len(ret), elapsed))


if __name__ == '__main__':
    main()
