import nx_pgnet
import osgeo.ogr as ogr
import time
import psycopg2
import fiona
from shapely.wkt import loads


def writeGraph(graph, netid):
    # Write the edges first
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "READ_TO_SHP//ITRC//Edges//"

    # write the network edges
    sourceSchema = {'properties': {'Length': 'float:19.11', 'type': 'str', 'feeder': 'str', 'netID': 'int'},  # NOQA
                    'geometry': 'LineString'}
    fileName = result_folder + 'Edges' + str(netid) + '.shp'
    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for edge in graph.edges():
            startNode = edge[0]
            endNode = edge[1]
            record = {}
            thisEdge = graph.edge[startNode][endNode]
            thisCoords = list(loads(thisEdge['Wkt']).coords)
            record['geometry'] = {'coordinates': thisCoords, 'type': 'LineString'}  # NOQA
            record['properties'] = {'Length': thisEdge['length'],
                                    'type': thisEdge['type'],
                                    'feeder': str(thisEdge['feeder']),
                                    'netID': netid}  # NOQA
            source.write(record)

    # write the nodes then
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "READ_TO_SHP//ITRC//Nodes//"

    # write the network edges
    sourceSchema = {'properties': {'type': 'str', 'toid': 'str', 'netID': 'int'},  # NOQA
                    'geometry': 'Point'}
    fileName = result_folder + 'Nodes' + str(netid) + '.shp'
    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for node in graph.nodes():
            thisNode = graph.node[node]
            record = {}
            p = loads(thisNode['Wkt'])
            record['geometry'] = {'coordinates': [p.x, p.y], 'type': 'Point'}  # NOQA
            record['properties'] = {'type': thisNode['type'], 'toid': thisNode['toid'], 'netID': netid}  # NOQA
            source.write(record)

start = time.time()

db_name = 'ITRC_NCL'

# first fetch names of all the graphs stored in the ITRC schema
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % db_name)  # NOQA
cur = conn.cursor()
cur.execute('SELECT "GraphName" from "Graphs"')
result = cur.fetchall()
cur.close()
conn.close()


# then use the graph name to fetch the graphs themselves
conn = ogr.Open("PG: host='localhost' dbname='%s' user='postgres' password='19891202'" % db_name, 1)  # NOQA

for i in range(len(result)):
    graphName = result[i][0]
    print(graphName)
    network = nx_pgnet.read(conn).pgnet(graphName)
    writeGraph(network, graphName)

conn = None

end = time.time()

cost = end - start

print("Spent %f seconds" % cost)
