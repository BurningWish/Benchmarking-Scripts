import os
import time
import shp2nx
import undir2dir
from neo4j.v1 import GraphDatabase

start = time.time()

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "19891202"))
with driver.session() as session:
    # first get all the substation names from the database
    result = session.run("MATCH (s:substation) return s.netid")
    temp_netids = []
    for data in result.data():
        temp_netids.append(int(data['s.netid']))
    temp_netids.sort()
    for netid in temp_netids:
        print(netid)
        session.run("MATCH p = shortestPath((s:substation)-[*]-(b:building)) \
                    where s.netid = %s and b.netid = %s return p" % (netid, netid))


end = time.time()

cost = end - start

print("Spent %f seconds" % cost)

