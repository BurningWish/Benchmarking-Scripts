import os
import time
import shp2nx
import undir2dir
from neo4j.v1 import GraphDatabase
import pickle
import networkx as nx
import psycopg2

start = time.time()

db_name = 'IRN_NCL_Hybrid'
citycat_DB = "11_ITRC_extra_data"
cat_model = "extr3_24_s5"

# Get Geometry of CatCAT
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % citycat_DB)  # NOQA
cur = conn.cursor()
cur.execute("select st_astext(geom) from %s" % cat_model)
citycat_wkt = cur.fetchall()[0][0]
cur.close()
conn.close()

# find flooded edges
flooded_eids = []
cur.execute("select eid from edges as e \
            where st_intersects(e.geom, st_geomfromtext('%s'))" % citycat_wkt)
results = cur.fetchall()
for result in results:
    flooded_eids.append(result[0])

disrupted_bids = []
problem_bids = []

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "19891202"))
with driver.session() as session:
    result = session.run("MATCH p = shortestPath((n1:centre)-[*]-(n2:buildings)) \
                        return p, n2.nid")
    
    for record in result:
        path = record['p']
        for e in flooded_eids:
            if e in path:
                nid = record['n2.nid']
                disrupted_bids.append(nid)
                break

# remove edge
with driver.session() as session:
    session.run("MATCH ()-[r]-() WHERE r.eid in '%s' DELETE r" % flooded_eids)
    
# re do path
ok_bids = []
with driver.session() as session:
    result = session.run("MATCH p = shortestPath((n1:centre)-[*]-(n2:buildings)) \
                        where n2.nid in '%s' return n2.nid" % disrupted_bids)
    for record in result:
        ok_bids = record['n2.nid']
    
problem_bids = list(set(disrupted_bids).difference(set(ok_bids)))


end = time.time()

cost = end - start

print("Spent %f seconds" % cost)