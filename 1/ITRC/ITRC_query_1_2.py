import nx_pgnet
import osgeo.ogr as ogr
import time
import psycopg2
import fiona
from shapely.wkt import loads
import networkx as nx


start = time.time()

db_name = 'ITRC_NCL'

# first fetch names of all the graphs stored in the ITRC schema
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % db_name)  # NOQA
cur = conn.cursor()
cur.execute('SELECT "GraphName" from "Graphs"')
result = cur.fetchall()
cur.close()
conn.close()

network = None

# then use the graph name to fetch the graphs themselves
conn = ogr.Open("PG: host='localhost' dbname='%s' user='postgres' password='19891202'" % db_name, 1)  # NOQA

for i in range(len(result)):
    graphName = result[i][0]
    print(graphName)
    network = nx_pgnet.read(conn).pgnet(graphName)

s_nodes = []
for n in network.nodes():
    if network.node[n]['type'] == 'substation':
        s_nodes.append(n)

for s_node in s_nodes:
    for n in network.nodes():
        nid = network.node[s_node]['netid']
        if network.node[n]['type'] == 'building' and network.node[n]['netid'] == netid:
            nx.shortest_path(network,s_node,n,weight = 'length')

conn = None

end = time.time()

cost = end - start

print("Spent %f seconds" % cost)
