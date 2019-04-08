import nx_pgnet
import osgeo.ogr as ogr
import networkx as nx
import os
import shp2nx
import undir2dir
import time

start = time.time()

db_name = "11_ITRC_NCL"

conn = ogr.Open("PG: host='localhost' dbname='%s' user='postgres' password='19891202'" % db_name, 1)  # NOQA

# Read PostGIS tables into Networkx instance into Python
edge_dir = "Special//Edges//"
node_dir = "Special//Nodes//"
for i in range(1, 1018):
    edge_path = edge_dir + "Edges" + str(i) + ".shp"
    node_path = node_dir + "Nodes" + str(i) + ".shp"
    if os.path.exists(edge_path) and os.path.exists(node_path):
        print(i)
        network = shp2nx.read_shp(edge_path, node_path)
        network = undir2dir.convert(network)
        nx_pgnet.write(conn).pgnet(network, "network%s"%str(i), srs=27700, overwrite=True)
conn = None

end = time.time()

cost = end - start

print("Spent %f seconds" % cost)