import psycopg2
import time
import os
import shp2nx
import undir2dir
from shapely.wkt import loads

"""
=====  Specify the location of our shapefile  =====
"""
edge_dir = "Lone//1600//Edges//"
node_dir = "Lone//1600//Nodes//"

dbname = "13_PR_1600"

"""
===== Initialize PostGIS connection
"""
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()
cur.execute("create extension if not exists postgis")
cur.execute("create extension if not exists pgrouting")
cur.execute("create extension if not exists postgis_topology")
cur.execute("create extension if not exists fuzzystrmatch")

cur.execute("drop table if exists edges")
cur.execute("drop table if exists edges_vertices_pgr")
cur.execute("create table edges (edge_id integer, net_id integer, type varchar(80), \
            feeder text, length double precision, direction text, geom geometry(LineString, 27700))")  # NOQA

pg_start = time.time()

"""
=======  Now Loop through the shapefiles and write edges to PostGIS  =======
"""
for i in range(1018):

    # Get the path of a network
    edge_path = edge_dir + "Edges" + str(i) + ".shp"
    node_path = node_dir + "Nodes" + str(i) + ".shp"

    # Only read the network if exsists such file
    if os.path.exists(edge_path) and os.path.exists(node_path):
        print(i, "pgRouting writing edges")
        undir_network = shp2nx.read_shp(edge_path, node_path)
        dir_network = undir2dir.convert(undir_network)
        dir_network = undir2dir.encodeDir(dir_network)

        net_id = i

        # now let's write all the edges in PostGIS
        for edge in dir_network.edges():
            startNode = edge[0]
            endNode = edge[1]
            edge_id = dir_network.edge[startNode][endNode]['edgeID']
            edge_type = dir_network.edge[startNode][endNode]['type']
            edge_length = dir_network.edge[startNode][endNode]['length']
            edge_wkt = dir_network.edge[startNode][endNode]['Wkt']
            edge_feeder = dir_network.edge[startNode][endNode]['feeder']

            # figure out the direction
            edge_coords = list(loads(edge_wkt).coords)
            start_point = edge_coords[0]
            end_point = edge_coords[-1]
            if dir_network.has_edge(start_point, end_point):
                direction = "FT"
            elif dir_network.has_edge(end_point, start_point):
                direction = "TF"
            else:
                direction = "UNKNOWN"
            #

            cur.execute("insert into edges (edge_id, net_id, type, feeder, length, direction, geom) \
            values (%d, %d, '%s', '%s', %f, '%s', st_geomfromtext('%s', 27700))" % (edge_id, net_id, edge_type, edge_feeder, edge_length, direction, edge_wkt))  # NOQA

"""
========  Create pgRouting topology  ============
"""
cur.execute("alter table edges add column source integer")
cur.execute("alter table edges add column target integer")
cur.execute("alter table edges add column gid serial primary key")
cur.execute("select pgr_createTopology('edges', 0.01, 'geom', 'gid')")
cur.execute("alter table edges_vertices_pgr add column type varchar(80)")
cur.execute("alter table edges_vertices_pgr add column net_id integer")
cur.execute("alter table edges_vertices_pgr add column toid varchar(80)")


"""
=== Now Loop through the shapefiles and assign nodes information to PostGIS ===
"""
for i in range(1018):

    # Get the path of a network
    edge_path = edge_dir + "Edges" + str(i) + ".shp"
    node_path = node_dir + "Nodes" + str(i) + ".shp"

    # Only read the network if exsists such file
    if os.path.exists(edge_path) and os.path.exists(node_path):
        print(i, "pgRouting assigning nodes information")
        undir_network = shp2nx.read_shp(edge_path, node_path)
        dir_network = undir2dir.convert(undir_network)
        dir_network = undir2dir.encodeDir(dir_network)

        net_id = i

        cur.execute("drop table if exists temp_nodes")
        cur.execute("create table temp_nodes (net_id integer, type varchar(80), \
                    toid varchar(80), geom geometry(Point, 27700))")  # NOQA
        """
        Now read the nodes (sub + buildings) information in a temporary table
        """

        for node in dir_network.nodes():
            this_node = dir_network.node[node]
            if this_node['type'] == 'substation':
                node_wkt = "POINT " + str(node).replace(",", " ")
                node_toid = this_node['toid']
                cur.execute("insert into temp_nodes (net_id, toid, type, geom) values  \
                 (%d, '%s', 'substation', st_geomfromtext('%s', 27700))" % (net_id, node_toid, node_wkt))  # NOQA
            elif this_node['type'] == 'building':
                node_wkt = "POINT " + str(node).replace(",", " ")
                node_toid = this_node['toid']
                cur.execute("insert into temp_nodes (net_id, toid, type, geom) values  \
                 (%d, '%s', 'building', st_geomfromtext('%s', 27700))" % (net_id, node_toid, node_wkt))  # NOQA
            else:
                node_wkt = "POINT " + str(node).replace(",", " ")
                node_toid = this_node['toid']
                cur.execute("insert into temp_nodes (net_id, toid, type, geom) values  \
                 (%d, '%s', 'other', st_geomfromtext('%s', 27700))" % (net_id, node_toid, node_wkt))  # NOQA

        cur.execute("update edges_vertices_pgr \
                    set type = old.type, net_id = old.net_id, toid = old.toid \
                    from temp_nodes as old \
                    where the_geom = old.geom")

        cur.execute("drop table if exists temp_nodes")

conn.commit()
cur.close()
conn.close()

pg_end = time.time()

pg_cost = pg_end - pg_start

print("pgRouting writing spent %f seconds" % pg_cost)
