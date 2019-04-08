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
====  Task 1 =====
"""

start = time.time()

net_DB = "11_ITRC_NCL"
fp_DB = "11_ITRC_extra_data"
fp_table = "random_fps"

# First fetch all the 100 footprints for circles
fp_list = []
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % fp_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % fp_table)
result = cur.fetchall()
for record in result:
    fp_list.append(record[0])

cur.close()
conn.close()

# Get names of all the network instances
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % net_DB)  # NOQA
cur = conn.cursor()
cur.execute('SELECT "GraphName" from "Graphs"')
graphs_result = cur.fetchall()

# For each circle, find the affected edges, then netids
"""
CHANGE the 0:50!!!!!!!!!!!!!!!
"""
fetched_graphs = []
for i in range(len(fp_list[0:50])):
    print("fp", i)
    fetched_graphs.append([])
    fp_wkt = fp_list[i]
    for j in range(len(graphs_result)):
        graphName = graphs_result[j][0]
        edge_table = graphName + "_Edge_Geometry"
        cur.execute("""select t1."GeomID" from "%s" as t1 \
                    where st_intersects(t1.geom, st_geomfromtext('%s', 27700))""" % (edge_table, fp_wkt))  # NOQA
        answer = cur.fetchall()
        if(len(answer)) > 0:
            fetched_graphs[i].append(graphName)

# Okay the fun part, loop through the fetched_graphs
affected_result = []
for i in range(len(fetched_graphs)):
    number_substations = 0
    number_buildings = 0
    networks = fetched_graphs[i]

    for graphName in networks:
        node_table = graphName + "_Nodes"
        cur.execute("""select * from "%s" where type = 'building'""" % node_table)  # NOQA
        result = cur.fetchall()
        number_substations += 1
        number_buildings += len(result)

    temp_dict = {}
    temp_dict['building_num'] = number_buildings
    temp_dict['substation_num'] = number_substations
    affected_result.append(temp_dict)

# Finally let's close the DB
cur.close()
conn.close()

end = time.time()

print("We have spent %s seconds" %(end-start))
