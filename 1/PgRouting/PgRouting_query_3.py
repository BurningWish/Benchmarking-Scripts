import psycopg2
import time
import pickle

db_name = '13_PR_NCL'

start = time.time()

ff = open("nid_pairs", "rb")
nid_pairs = pickle.load(ff)
ff.close()


conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % db_name)  # NOQA
cur = conn.cursor()

"""
=====  First find how many network instances are there  =====
"""

path_result = {}

net_ids = []
cur.execute("select distinct on (net_id) net_id\
            from edges order by net_id")
result = cur.fetchall()
for record in result:
    net_ids.append(record[0])

"""
=====  Then loop through each network instance  =====
"""
for net_id in net_ids:
    print("pure graph query for network %d" % net_id)
    """
    1. resolve dijkstra path in a nodal pair
    """
    for pair in nid_pairs:
        cur.execute("select * from pgr_dijkstra(\
                    'select gid as id,\
                    source::integer,\
                    target::integer,\
                    length::double precision as cost\
                    from edges',\
                    %d, %d, FALSE)" % (pair[0], pair[1]))
    result = cur.fetchall()
    path_result[net_id] = result

end = time.time()

pr_cost = end - start
print("pgRouting graph spent %f seconds" % pr_cost)
