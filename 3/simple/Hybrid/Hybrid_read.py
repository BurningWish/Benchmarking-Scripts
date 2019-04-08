import fiona
import time
import networkx as nx
from neo4j.v1 import GraphDatabase
from shapely.wkt import loads
import psycopg2

"""
Specify the PostGIS database we want to visit
"""
dbname = "FED_80000"


def writeNodesToShp(nodes, node_geoms, netid):
    # write the nodes then
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "READ_TO_SHP//FED//Nodes//"

    # write the network edges
    sourceSchema = {'properties': {'type': 'str',
                                   'toid': 'str',
                                   'netID': 'int',
                                   'sector': 'str',
                                   'utilityid': 'str'},  # NOQA
                    'geometry': 'Point'}
    fileName = result_folder + 'Nodes' + str(netid) + '.shp'
    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for n in nodes:
            thisProp = n['n'].properties
            nid = thisProp['nodeID']
            record = {}
            thisWkt = node_geoms[nid][1]
            p = loads(thisWkt)
            record['geometry'] = {'coordinates': [p.x, p.y], 'type': 'Point'}
            record['properties'] = {'type': thisProp['type'],
                                    'toid': thisProp['toid'],
                                    'netID': netid,
                                    'sector': thisProp['sector'],
                                    'utilityid': thisProp['utilityid']}  # NOQA
            source.write(record)


def writeEdgesToShp(edges, edge_geoms, netid):
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "READ_TO_SHP//FED//Edges//"

    # write the network edges
    sourceSchema = {'properties': {'Length': 'float:19.11', 'type': 'str', 'feeder': 'str', 'netID': 'int'},  # NOQA
                    'geometry': 'LineString'}
    fileName = result_folder + 'Edges' + str(netid) + '.shp'
    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for r in edges:
            thisProp = r['r'].properties
            eid = thisProp['edgeID']
            record = {}
            thisWkt = edge_geoms[eid][1]
            l = loads(thisWkt)
            record['geometry'] = {'coordinates': list(l.coords), 'type': 'LineString'}  # NOQA
            record['properties'] = {'Length': thisProp['length'],
                                    'type': thisProp['type'],
                                    'feeder': str(thisProp['feeder']),
                                    'netID': netid}  # NOQA
            source.write(record)


start = time.time()

"""
=========  First lets connect the PostGIS database
"""

conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()

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

        # Use a networkx instance to store information
        network = nx.DiGraph()

        """
        =========================  First dealing with Nodes  ==================
        """
        # Fetch the node geometry result
        cur.execute("SELECT node_id, st_astext(geom) from nodes where net_id = %d order by node_id" % netid)  # NOQA
        node_geoms = cur.fetchall()

        # Okay retrieve all the nodes and write them to shp
        result = session.run("MATCH (n{netid:%d}) return n" % netid)
        nodes = []
        for data in result.data():
            nodes.append(data)

        # Now write nodes
        writeNodesToShp(nodes, node_geoms, netid)

        """
        ========================  Then dealing with edges  ====================
        """
        # Fetch the edge geometry result
        cur.execute("SELECT edge_id, st_astext(geom) from edges where net_id = %d order by edge_id" % netid)  # NOQA
        edge_geoms = cur.fetchall()

        # Then retrieve all the edges and write them to shp +++
        result = session.run("MATCH (m)-[r{netid:%d}]->(n) return r" % netid)  # NOQA
        edges = []
        for data in result.data():
            edges.append(data)

        # Now write edges
        writeEdgesToShp(edges, edge_geoms, netid)


conn.close()
end = time.time()

cost = end - start

print("Spent %f seconds" % cost)
