from concurrent import futures
import time

try:
    from . import db
except Exception as e:
    import db


with open('sample.csv', 'r') as fp:
        glass_id = fp.readlines()

glass_id = [i.rstrip() for i in glass_id]


MAX_WORKER = 200
ret = []

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



def query_many(glass_id):
    workers = min(MAX_WORKER, len(glass_id))
    with futures.ThreadPoolExecutor(workers) as execute:
        res = execute.map(query, sorted(glass_id))
    ret.append(res)    
    return len(list(res))



def main(query_many):
    t0 = time.time()
    count = query_many(glass_id)
    elapsed = time.time() - t0
    msg = '\n{} glass_id query in {:.2f}s'
    print(msg.format(count, elapsed))


if __name__ == '__main__':
    main(query_many)





