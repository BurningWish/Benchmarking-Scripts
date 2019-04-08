"""
This script uses the citycat footprint, it does four things

1. Find all the affected substations, then buildings
2. Find all the affected buildings, then substations
3. Find all the non affected substations, then buildings
4. Find all the non affected buildings, then substations
"""

import psycopg2
import time
import os
import shp2nx
import undir2dir

net_DB = "13_PR_NCL"
fp_DB = "13_PR_extra_data"
fp_table = "random_fps"

"""
====  Task 1  ====
"""

start = time.time()

# First fetch all the 1000 footprints for circles
fp_list = []
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % fp_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % fp_table)
result = cur.fetchall()
for record in result:
    fp_list.append(record[0])

conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
cur = conn.cursor()
# Find all the affected substations
for fp in fp_list:
    affected_netids = []
    cur.execute("select net_id from edges_vertices_pgr as v \
                where st_intersects(v.the_geom, st_geomfromtext('%s', 27700)) \
                and v.type = 'substation'" % fp)  # NOQA
    result = cur.fetchall()
    for record in result:
        affected_netids.append(record[0])
    
    # Now it is time to use the netid to fetch all the buildings in the network
    for net_id in affected_netids:
        cur.execute("select * from edges_vertices_pgr where net_id = '%d' and \
                    type = 'building'" % net_id)
        temp_building_result = cur.fetchall()

cur.close()
conn.close()

end = time.time()

print("We have spent %s seconds" %(end-start))