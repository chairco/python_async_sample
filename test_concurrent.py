# -*- coding:utf-8 -*-
import os
import time
import multiprocessing

from datetime import datetime

from concurrent import futures

from collections import namedtuple

from itertools import chain

try:
    from . import db
    from . import auto
except Exception as e:
    import db
    import auto


Result = namedtuple('Result', 'data')


with open('sample.csv', 'r') as fp:
    glass_id = fp.readlines()

glass_id = [i.rstrip() for i in glass_id]

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

MAX_WORKER = 200


def query_many_map(query, glass_id):
    """
    test mutipthread query with .map 
    """
    workers = min(MAX_WORKER, len(glass_id))
    with futures.ThreadPoolExecutor(workers) as execute:
        res = execute.map(query, sorted(glass_id))
    return res


def query_many(query, glass_id):
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


def query_edc_data_many(query, datas):
    """
    Query oracle db by mutipleprocess
    :type query: query object
    :type datas: list
    :rtype dict()  
    """
    with futures.ProcessPoolExecutor() as executor:
        future_to_sid = {
            executor.submit(
                query, value[0], value[1], value[2]): value[1] for value in datas
        }
        result = {}
        for future in futures.as_completed(future_to_sid):
            s_id = future_to_sid[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (s_id, exc))
            else:
                #print('%r step_id has %d rows' % (s_id, len(data)))
                pass
            result.setdefault(s_id, data)
    return result


def query_edc_data_many_mulit(query, datas, results_dict=None):
    """
    Query oracle db by mutipleprocess
    :type query: query object
    :type datas: list
    :rtype dict()  
    """
    with futures.ProcessPoolExecutor() as executor:
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
            if results_dict != None: results_dict[g_s_id] = data
    return result


def query_edc_data_many_dict(query, datas):
    """
    Query oracle db by mutipleprocess
    :type query: query object
    :type datas: list
    :rtype dict()  
    """
    result = {}
    workers = min(30, len(datas))
    with futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for key, values in datas.items():
            future_to_sid = {
                executor.submit(
                    query, value[0], value[1], value[2]): value[1] for value in values
            }
            res = {}
            for future in futures.as_completed(future_to_sid):
                s_id = future_to_sid[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (s_id, exc))
                else:
                    #print('%r step_id has %d rows' % (s_id, len(data)))
                    pass
                res.setdefault(s_id, data)
            result.setdefault(key, res)
    return result


def query_edc_currency(query, datas):
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


def edc_data():
    """caculate on here
    this is for yield from
    """
    while True:
        term = yield
        if term is None:
            break
        result = auto.get_edc_data(term[0], term[1], term[2])
    return Result(result)


def grouper(result, key):
    """
    proxy just for test yield from
    """
    while True:
        result[key] = yield from edc_data()


def report(g_id, datas):
    """
    This output csv file under root/output/
    :type g_id: str
    :type datas: dict()
    """
    path = os.path.join(BASE_DIR, g_id + '.csv')
    with open(path, 'w') as fp:
        for key, values in datas.items():
            for value in values:
                value = list(map(str, value))
                for i in range(len(value)):
                    if isinstance(value[i], datetime):
                        value[i] = (value[i].strftime('%Y/%m/%d %H:%M:%S'))
                fp.write(', '.join(value))
                fp.write('\n')


def main(query, query_many):
    t0 = time.time()
    ret = query_many(query, glass_id)
    elapsed = time.time() - t0
    msg = '\n{} glass_id query in {:.2f}s'
    print(msg.format(len(ret), elapsed))
    return ret


def main_crazy_future(ret):
    print('\n #### main_crazy_future() ####')
    t0 = time.time()
    # Query by thread 7s BUT process 2~3s
    results = query_edc_data_many_dict(auto.get_edc_data, ret)
    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))


def main_future(ret):
    print('\n #### main_future() ####')
    results = {}
    t0 = time.time()
    for key, values in ret.items():
        t1 = time.time()
        # Query by thread 7s BUT process 2~3s
        result = query_edc_data_many(auto.get_edc_data, values)
        results.setdefault(key, result)

        #msg = '\n{}, {} each glass_id_dec_step_id query in {:.2f}s'
        #print(msg.format(key, len(result), time.time() - t1))

    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))

    # output csv files
    for key, values in results.items():
        report(g_id=key, datas=values)


def main_yield_from(ret):
    print('\n #### main_yield_from() ####')
    results = {}
    t0 = time.time()
    for key, values in ret.items():
        t1 = time.time()
        msg = '\n{}, {} each glass_id_dec_step_id query in {:.2f}s'
        # yield from
        group = grouper(results, key)
        next(group)
        for value in values:
            print('send value: {}'.format(value))
            group.send(value)
        group.send(None)
        print(msg.format(key, len(results), time.time() - t1))

    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))


def report_for_multi(results_dict):
    # output csv files. using a+
    print('start write csv file.')
    for key, values in results_dict.items():
        g_id, sid = key.split('_')
        path = os.path.join(BASE_DIR, g_id + '.csv')
        with open(path, 'a+') as fp:
            for value in values:
                value = list(map(str, value))
                for i in range(len(value)):
                    if isinstance(value[i], datetime):
                        value[i] = (value[i].strftime('%Y/%m/%d %H:%M:%S'))
                fp.write(', '.join(value))
                fp.write('\n')
    print('write csv file done.')


def main_multiprocess(ret):
    print('\n #### main_multiprocess() ####')
    multiprocessing.freeze_support()
    pool = multiprocessing.Pool()

    manager = multiprocessing.Manager()
    results_dict = manager.dict()  # share data
    record = []
    lock = multiprocessing.Lock()

    t0 = time.time()
    for key, values in ret.items():
        # mutiProcess
        process = multiprocessing.Process(
            target=query_edc_data_many_mulit,
            args=(auto.get_edc_data, values, results_dict)
        )
        process.start()
        record.append(process)

    for process in record:
        process.join()

    gid_list = []
    for key, value in results_dict.items():
        gid, sid = key.split('_')
        if gid not in gid_list:
            gid_list.append(gid)

    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(gid_list), elapsed_edc))

    # report
    #report_for_multi(results_dict) 


def main_pool(ret):
    # should set <export PYTHONOPTIMIZE=1>
    print('\n #### main_pool() ####')
    multiprocessing.freeze_support()
    pool = multiprocessing.Pool()
    results = []
    
    t0 = time.time()
    
    for key, values in ret.items():
        result = pool.apply_async(query_edc_data_many_mulit, args=(auto.get_edc_data, values,))
        results.append(result)
    
    pool.close()
    pool.join()
    
    # data structure:
    # [{gid1_sid:[raw_data], gid1_sid:[row_data]}, {gid1_sid:[raw_data], gid1_sid:[row_data]}]
    datas = [result.get() for result in results]

    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))


def main_concurrent(ret):
    print('\n #### main_concurrent() ####')
    t0 = time.time()
    
    values = list(chain.from_iterable(ret.values()))
    results = query_edc_currency(auto.get_edc_data, values)

    elapsed_edc = time.time() - t0
    msg = '\n{} All glass_id_dec_step_id query in {:.2f}s'
    print(msg.format(len(results), elapsed_edc))


if __name__ == '__main__':
    ret = main(auto.get_edc_glass_history, query_many)
    main_yield_from(ret) # slow
    main_crazy_future(ret) # normal
    main_future(ret) # normal
    main_pool(ret) # fast
    main_multiprocess(ret) # fast
    main_concurrent(ret) # fast
    