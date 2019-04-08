"""
This script uses the citycat footprint, it does four things

1. Find all the affected substations, then buildings
2. Find all the affected buildings, then substations
3. Find all the non affected substations, then buildings
4. Find all the non affected buildings, then substations
"""

import os
import time
import shp2nx
import undir2dir
import psycopg2
from neo4j.v1 import GraphDatabase

"""
====  Task 4  ====
"""

start = time.time()

net_DB = "14_FED_NCL"
fp_DB = "14_FED_extra_data"
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

# Find all the affected network from the footprint (has building inside)
for fp in fp_list:
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
    cur = conn.cursor()
    cur.execute("select distinct on (net_id) net_id from nodes as n where n.type = 'building' and \
                st_intersects(n.geom, st_geomfromtext('%s', 27700))" % fp)  # NOQA
    result = cur.fetchall()
    affected_netids = []
    for record in result:
        affected_netids.append(record[0])
    
    # Find all the possible netids
    cur.execute("select net_id from nodes as n where n.type = 'substation'")  # NOQA
    result = cur.fetchall()
    possible_netids = []
    for record in result:
        possible_netids.append(record[0])
    
    # Find the unaffected networks
    unaffected_netids = list((set(possible_netids)) - set(affected_netids))
    
    # Now let's find all the unaffected substations from these unaffected networks
    for net_id in unaffected_netids:
        cur.execute("select * from nodes as n where n.type = 'substation' \
                    and n.net_id = '%d'" % net_id)
        temp_substation_result = cur.fetchall()


# Finall let's close the database
cur.close()
conn.close()

end = time.time()

print("We have spent %s seconds" %(end-start))
