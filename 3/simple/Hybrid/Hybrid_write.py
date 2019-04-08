import os
import time
import shp2nx
import undir2dir
import psycopg2
from neo4j.v1 import GraphDatabase
import sys


"""
=====  Specify the location of our shapefile  =====
"""
edge_dir = "NCL//Edges//"
node_dir = "NCL//Nodes//"

"""
=====  Specify our postgis database  =====
"""
dbname = "14_FED_NCL"

"""
=====  Specify our neo4j database  =====
"""
uri = "bolt://localhost:7687"

"""
===== Initialize PostGIS connection
"""
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()
cur.execute("drop table if exists nodes")
cur.execute("drop table if exists edges")
cur.execute("create table nodes (node_id integer, net_id integer, \
            type text, toid text, sector text, utilityid text, geom geometry(Point, 27700))")  # NOQA
cur.execute("create table edges (edge_id integer, net_id integer, \
            length real, type varchar(80), feeder text, geom geometry(LineString, 27700))")  # NOQA
cur.execute("CREATE INDEX edge_index ON edges USING GIST (geom)")
cur.execute("CREATE INDEX node_index ON nodes USING GIST (geom)")

pg_start = time.time()

"""
=======  Now Loop through the shapefiles and write networks to PostGIS  =======
"""
for i in range(1018):

    # Get the path of a network
    edge_path = edge_dir + "Edges" + str(i) + ".shp"
    node_path = node_dir + "Nodes" + str(i) + ".shp"

    # Only read the network if exsists such file
    if os.path.exists(edge_path) and os.path.exists(node_path):
        print(i, "PostGIS")
        undir_network = shp2nx.read_shp(edge_path, node_path)
        dir_network = undir2dir.convert(undir_network)
        dir_network = undir2dir.encodeDir(dir_network)

        net_id = i

        # first write all the nodes in PostGIS
        for node in dir_network.nodes():
            node_id = dir_network.node[node]['nodeID']
            node_wkt = "POINT " + str(node).replace(",", " ")
            node_type = dir_network.node[node]['type']
            node_toid = dir_network.node[node]['toid']
            node_sector = dir_network.node[node]['sector']
            node_utilityId = dir_network.node[node]['utilityId']
            cur.execute("insert into nodes (node_id, net_id, type, toid, sector, utilityid, geom) \
            values (%d, %d, '%s', '%s', '%s', '%s', st_geomfromtext('%s', 27700))" % (node_id, net_id, node_type, node_toid, node_sector, node_utilityId, node_wkt))   # NOQA

        # now let's write all the edges in PostGIS
        for edge in dir_network.edges():
            startNode = edge[0]
            endNode = edge[1]
            edge_id = dir_network.edge[startNode][endNode]['edgeID']
            edge_wkt = dir_network.edge[startNode][endNode]['Wkt']
            edge_length = dir_network.edge[startNode][endNode]['length']
            edge_type = dir_network.edge[startNode][endNode]['type']
            edge_feeder = dir_network.edge[startNode][endNode]['feeder']
            cur.execute("insert into edges (edge_id, net_id, length, type, feeder, geom) \
            values (%d, %d, '%f', '%s', '%s', st_geomfromtext('%s', 27700))" % (edge_id, net_id, edge_length, edge_type, edge_feeder, edge_wkt))  # NOQA

# Create spatial index on node and edge geometry


# I am still debating whether to commit for each network or just once...
conn.commit()
conn.close()

neo_start = time.time()

"""
===================  Then Write all the networs into Neo4j  ===================
"""

driver = GraphDatabase.driver(uri, auth=("neo4j", "19891202"))
with driver.session() as session:

    # Loop through the shapefiles to write every network to Neo4j
    for i in range(1018):
        edge_path = edge_dir + "Edges" + str(i) + ".shp"
        node_path = node_dir + "Nodes" + str(i) + ".shp"

        # Only read the network from shapefile if exists such one
        if os.path.exists(edge_path) and os.path.exists(node_path):
            print(i, "Neo4j")
            undir_network = shp2nx.read_shp(edge_path, node_path)
            dir_network = undir2dir.convert(undir_network)
            dir_network = undir2dir.encodeDir(dir_network)

            net_id = i

            # first write all the nodes in Neo4j
            for node in dir_network.nodes():
                label = dir_network.node[node]['type']
                param_dict = dir_network.node[node]
                session.run(statement="CREATE (n:%s {params})" % label,
                            parameters={"params": param_dict})

            session.run("CREATE INDEX ON :substation(nodeID)")
            session.run("CREATE INDEX ON :substationAccess(nodeID)")
            session.run("CREATE INDEX ON :building(nodeID)")
            session.run("CREATE INDEX ON :buildingAccess(nodeID)")
            session.run("CREATE INDEX ON :distribution(nodeID)")

            # secondly write all the edges(relationships in Neo4j)
            for edge in dir_network.edges():
                startNode = edge[0]
                endNode = edge[1]
                this_edge = dir_network.edge[startNode][endNode]
                label = this_edge['type']
                from_node_id = this_edge['fromNodeID']
                to_node_id = this_edge['toNodeID']
                from_node_label = this_edge['fromNodeLabel']
                to_node_label = this_edge['toNodeLabel']
                param_dict = this_edge
                del param_dict['Wkb']
                del param_dict['Wkt']

                query = """MATCH (n1:%s {nodeID:%d, netid:%d}), 
                                 (n2:%s {nodeID:%d, netid:%d}) 
                                 CREATE (n1)-[:%s {params}]->(n2)""" % (from_node_label, from_node_id, net_id, to_node_label, to_node_id, net_id, label)  # NOQA

                # use the MERGE doesn't seem to be different
                # query = """MERGE (n1:%s {nodeID:%d, netid:%d})
                #           MERGE (n2:%s {nodeID:%d, netid:%d})
                #           CREATE (n1)-[:%s {params}]->(n2)""" % (from_node_label, from_node_id, net_id, to_node_label, to_node_id, net_id, label)  # NOQA

                session.run(statement=query,
                            parameters={"params": param_dict})

end = time.time()

pg_cost = neo_start - pg_start
neo_cost = end - neo_start

print("PostGIS writing spent %f seconds" % pg_cost)
print("Neo4j writing spent %f seconds" % neo_cost)
