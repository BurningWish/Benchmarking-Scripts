import nx_pgnet
import osgeo.ogr as ogr
import time
import psycopg2
import fiona
from shapely.wkt import loads
import network as nx

start = time.time()

db_name = 'IRN_NCL'
citycat_DB = "11_ITRC_extra_data"
cat_model = "extr3_24_s5"

# Get Geometry of CatCAT
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % citycat_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % cat_model)
citycat_wkt = cur.fetchall()[0][0]
cur.close()
conn.close()

conn1 = ogr.Open("PG: host='localhost' dbname='%s' user='postgres' password='19891202'" % db_name, 1)  # NOQA
conn2 = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % db_name)  # NOQA
cur = conn2.cursor()

IRN = nx_pgnet.read(conn1).pgnet('IRN')

# find flooded edges
flooded_eids = []
cur.execute("select eid from edges as e \
            where st_intersects(e.geom, st_geomfromtext('%s'))" % citycat_wkt)
results = cur.fetchall()
for result in results:
    flooded_eids.append(result[0])

# find centre
centre_node = None
for n in IRN.nodes():
    if IRN.node[n]['type'] == 'centre':
        centre_node = n
        break
 
# find disrupted buildings
disrupted_bids = []
flooded_edges = []
for n in IRN.nodes():
    if IRN.node[n]['type'] == 'building':
        path = nx.shortest_path(IRN, n, centre_node)
        for e in path:
            if IRN.edge[e[0]][e[1]]['eid'] in flooded_eids:
                disrupted_bids.append(n)
                flooded_edges.append(e)
                break

# modify IRN
for e in flooded_edges:
    IRN.remove_edge(e)

# let re do Dijkstra path
problem_bids = []
for bid in disrupted_bids:
    if not nx.has_path(IRN, bid, centre_node):
        problem_bids.append(bid)
    
conn1 = None
conn2 = None

end = time.time()

cost = end - start

print("Spent %f seconds" % cost)