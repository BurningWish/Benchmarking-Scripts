import psycopg2
import time

db_name = '13_PR_NCL'

start = time.time()

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
    1. find the substation node id
    """
    cur.execute("select id from edges_vertices_pgr\
                where type = 'substation'\
                and net_id = %d" % net_id)
    asset_id = cur.fetchall()[0][0]

    """
    2. find the buildings' ids for this network
    """
    cur.execute("select id from edges_vertices_pgr\
                where type = 'building'\
                and net_id = %d" % net_id)
    result = cur.fetchall()
    bid_list = []
    for record in result:
        bid_list.append(record[0])

    """
    3. resolve dijkstra path from the asset to every building
    """
    cur.execute("select * from pgr_dijkstra(\
                'select gid as id,\
                source::integer,\
                target::integer,\
                length::double precision as cost\
                from edges',\
                %d, array%s, FALSE)" % (asset_id, bid_list))
    result = cur.fetchall()
    path_result[net_id] = result

end = time.time()

pr_cost = end - start
print("pgRouting graph spent %f seconds" % pr_cost)
