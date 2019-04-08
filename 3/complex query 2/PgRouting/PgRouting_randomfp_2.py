"""
This script uses the random footprint, it does two things

1. Find all the affected edges, then buildings, substations
2. Find all the non affected edges, the buildings, substations
"""

import psycopg2
import nx_pgnet
import osgeo.ogr as ogr
import os
import shp2nx
import undir2dir
import time
from shapely.wkt import loads

"""
====  Task 2 =====
"""
start = time.time()

net_DB = "13_PR_NCL"
fp_DB = "13_PR_extra_data"
fp_table = "random_fps"

# First fetch all the 1000 footprints for circles
fp_list = []
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % fp_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % fp_table)
result = cur.fetchall()
for record in result:
    fp_list.append(record[0])

cur.close()
conn.close()


conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
cur = conn.cursor()

# Now first find all the possible net_ids
possible_netids = []
cur.execute("select distinct on (net_id) net_id from edges")
result = cur.fetchall()
for record in result:
    possible_netids.append(record[0])

# Now let's loop through each fp, find unaffected networks and then substations / buildings  # NOQA

unaffected_result = []

for i in range(len(fp_list[0:50])):
    print("fp", i)
    fp_wkt = fp_list[i]
    affected_netids = []
    cur.execute("select distinct on (net_id) net_id from edges as e \
                where st_intersects(e.geom, st_geomfromtext('%s', 27700))" % fp_wkt)  # NOQA
    result = cur.fetchall()
    for record in result:
        affected_netids.append(record[0])

    unaffected_netids = set(possible_netids) - set(affected_netids)

    temp_dict = {}

    # Find affected substations
    cur.execute("select * from edges_vertices_pgr as v where v.type = 'substation' \
                and v.net_id in %s" % str(tuple(unaffected_netids)))  # NOQA
    result = cur.fetchall()
    temp_dict['substation_num'] = len(result)

    # Find affected buildings
    cur.execute("select * from edges_vertices_pgr as v where v.type = 'building' \
                and v.net_id in %s" % str(tuple(unaffected_netids)))  # NOQA
    result = cur.fetchall()
    temp_dict['building_num'] = len(result)

    unaffected_result.append(temp_dict)

# Finally let's close the DB
cur.close()
conn.close()

end = time.time()
print("We have spent %s seconds" %(end-start))