import time

from concurrent import futures

try:
    from . import db
except Exception as e:
    import db


with open('sample.csv', 'r') as fp:
    glass_id = fp.readlines()

glass_id = [i.rstrip() for i in glass_id]


MAX_WORKER = 200


def query(glass_id):
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT * 
        FROM lcdsys.array_pds_glass_t t
        WHERE 1=1 AND t.glass_id = :glass_id
        ORDER BY glass_start_time
        """,
        {'glass_id': glass_id}
    )
    rows = cursor.fetchall()
    return rows


def query_many2(glass_id):
    workers = min(MAX_WORKER, len(glass_id))
    with futures.ThreadPoolExecutor(workers) as execute:
        res = execute.map(query, sorted(glass_id))
    return res


def query_many(glass_id):
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKER) as executor:
        to_do = []
        for g_id in sorted(glass_id):
            future = executor.submit(query, g_id)
            to_do.append(future)
            msg = 'Scheduled for {}: {}'
            #print(msg.format(g_id, future))

        result = []
        for future in futures.as_completed(to_do):
            res = future.result()
            msg = '{} result: {!r}'
            #print(msg.format(future, res))
            result.append(res)

    return len(result)


def main(query_many):
    t0 = time.time()
    count = query_many(glass_id)
    elapsed = time.time() - t0
    msg = '\n{} glass_id query in {:.2f}s'
    print(msg.format(count, elapsed))


if __name__ == '__main__':
    main(query_many)
