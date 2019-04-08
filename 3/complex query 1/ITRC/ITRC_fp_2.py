"""
This script uses the citycat footprint, it does four things

1. Find all the affected substations, then buildings
2. Find all the affected buildings, then substations
3. Find all the non affected substations, then buildings
4. Find all the non affected buildings, then substations
"""

import sys
import psycopg2
import nx_pgnet
import osgeo.ogr as ogr
import os
import shp2nx
import undir2dir
import time
from shapely.wkt import loads

net_DB = "11_ITRC_NCL"
fp_DB = "11_ITRC_extra_data"
fp_table = "random_fps"

"""
=======  Task 2  ===========
"""

start = time.time()

# First fetch all the 100 footprints for circles
fp_list = []
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % fp_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % fp_table)
result = cur.fetchall()
for record in result:
    fp_list.append(record[0])

# Get names of all the network instances
for fp in fp_list:
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
    cur = conn.cursor()
    cur.execute('SELECT "GraphName" from "Graphs"')
    graphs_result = cur.fetchall()
    
    # For each network instance loop
    fetched_netids = []
    temp_answer = []
    for i in range(len(graphs_result)):
        graphName = graphs_result[i][0]
        print(graphName)
        node_table = graphName + "_Nodes"
        cur.execute("""select t1.type, t1.netid from "%s" as t1 \
                    where t1.type = 'building' and \
                    st_dwithin(t1.geom, st_geomfromtext('%s', 27700), 2)""" % (node_table, fp))  # NOQA
        answer = cur.fetchall()
        for ans in answer:
            netid = ans[1]
            if netid not in fetched_netids:
                fetched_netids.append(netid)
    
    
    # Use fetched_netids to find the substation nodes
    affected_substations = []
    for netid in fetched_netids:
        print(netid)
        node_table = "network" + str(netid) + "_Nodes"
        cur.execute("""select * from "%s" where type = 'substation'""" % node_table)  # NOQA
        fetched_substation = cur.fetchall()[0]
        affected_substations.append(fetched_substation)
    
    
    # Finally close the database
    cur.close()
    conn.close()

end = time.time()

print("We have spent %s seconds" %(end-start))
