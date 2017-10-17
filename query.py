# -*- coding:utf-8 -*-
import os
import time
import multiprocessing
import inspect

from functools import wraps
from datetime import datetime
from concurrent import futures
from itertools import chain

import auto

from cktypes import checktypes


BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

MAX_WORKER = 200


class Queryedc:
    """
    Here is API for query oracle db whith high level multiprocess. 
    :types query: auto.query object
    """
    def __init__(self):
        pass
    
    def _query_histroy_concurrency(self, query, glass_id):
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

    @checktypes
    def glass_history(self, glass_id: list):
        return self._query_histroy_concurrency(auto.get_edc_glass_history, glass_id)

    @checktypes
    def glass_data(self, glass_id: list):
        sid_dataset = self._query_histroy_concurrency(auto.get_edc_glass_history, glass_id)
        values = list(chain.from_iterable(sid_dataset.values()))
        return self._query_data_concurrency(auto.get_edc_data, values)


if __name__ == '__main__':
    with open('sample.csv', 'r') as fp:
        glass_id = fp.readlines()
    glass_id = [i.rstrip() for i in glass_id]

    t0 = time.time()

    q = Queryedc()
    #q.glass_history(glass_id)
    results = q.glass_data(glass_id)
    
    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))
    
