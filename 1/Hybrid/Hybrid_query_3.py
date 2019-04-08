import os
import time
import shp2nx
import undir2dir
from neo4j.v1 import GraphDatabase
import pickle

start = time.time()

ff = open("nid_pairs", "rb")
nid_pairs = pickle.load(ff)
ff.close()


uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "19891202"))
with driver.session() as session:
    for pair in nid_pairs:
        session.run("MATCH p = shortestPath((n1)-[*]-(n2)) \
                    where n1.nid = %s and n2.nid = %s return p" % (pair[0], pair[1]))


end = time.time()

cost = end - start

print("Spent %f seconds" % cost)

