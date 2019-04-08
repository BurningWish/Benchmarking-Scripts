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

net_DB = "14_FED_NCL"
citycat_DB = "14_FED_extra_data"
cat_model = "extr3_24_s5"

"""
====  Task 2  ====
"""
start = time.time()
# Fetch the citycat_footprint
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % citycat_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % cat_model)
citycat_wkt = cur.fetchall()[0][0]
cur.close()
conn.close()

# Find all the affect building nodes within the footprint
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
cur = conn.cursor()
cur.execute("select distinct on (net_id) net_id from nodes as n where n.type = 'building' and \
            st_intersects(n.geom, st_geomfromtext('%s', 27700))" % citycat_wkt)  # NOQA
result = cur.fetchall()
affected_netids = []
for record in result:
    affected_netids.append(record[0])

# Now let's find all the affected substation for these affected_netids
for net_id in affected_netids:
    cur.execute("select * from nodes as n where n.type = 'substation' \
                and n.net_id = '%d'" % net_id)
    temp_substation_result = cur.fetchall()

# Finall let's close the database
cur.close()
conn.close()

end = time.time()

print("We have spent %s seconds" %(end-start))
