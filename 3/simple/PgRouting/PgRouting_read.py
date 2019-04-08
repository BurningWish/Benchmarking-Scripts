import psycopg2
import time
from shapely.wkt import loads
import fiona

db_name = '13_PR_NCL'

start = time.time()

# first fetch the graph name appears in the database
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % db_name)  # NOQA
cur = conn.cursor()
cur.execute("select distinct on(net_id) net_id from edges order by net_id")
result = cur.fetchall()

for i in range(len(result)):
    net_id = result[i][0]
    print("reading network %d" % net_id)
    """
    =====  First let's write the edges  =====
    """
    result_folder = "READ_TO_SHP//PR//Edges//"

    cur.execute("select type, length, st_astext(geom), feeder from edges \
                where net_id = %d" % net_id)

    edges_result = cur.fetchall()

    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}
    sourceSchema = {'properties': {'Length': 'float:19.11',
                                   'type': 'str',
                                   'feeder': 'str',
                                   'netID': 'int'},  # NOQA
                    'geometry': 'LineString'}
    fileName = result_folder + 'Edges' + str(net_id) + '.shp'

    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for edge_record in edges_result:
            edge_type = edge_record[0]
            edge_length = edge_record[1]
            edge_wkt = edge_record[2]
            edge_feeder = edge_record[3]
            edge_coords = list(loads(edge_wkt).coords)
            record = {}
            record['geometry'] = {'coordinates': edge_coords, 'type': 'LineString'}  # NOQA
            record['properties'] = {'Length': edge_length,
                                    'type': edge_type,
                                    'feeder': edge_feeder,
                                    'netID': net_id}  # NOQA
            source.write(record)

    """
    =====  Now let's write the nodes  =====
    """
    result_folder = "READ_TO_SHP//PR//Nodes//"

    cur.execute("select type, st_astext(the_geom), toid, sector, utilityid from edges_vertices_pgr \
                where net_id = %d" % net_id)

    nodes_result = cur.fetchall()

    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}
    sourceSchema = {'properties': {'type': 'str', 
                                   'toid': 'str',
                                   'netID': 'int',
                                   'sector': 'str',
                                   'utilityid': 'str'},  # NOQA
                    'geometry': 'Point'}

    fileName = result_folder + 'Nodes' + str(net_id) + '.shp'

    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for node_record in nodes_result:
            node_type = node_record[0]
            node_wkt = node_record[1]
            node_toid = node_record[2]
            node_sector = node_record[3]
            node_utilityid = node_record[4]
            record = {}
            p = loads(node_wkt)
            record['geometry'] = {'coordinates': [p.x, p.y], 'type': 'Point'}  # NOQA
            record['properties'] = {'type': node_type,
                                    'toid': node_toid,
                                    'netID': net_id,
                                    'sector': node_sector,
                                    'utilityid': node_utilityid}  # NOQA
            source.write(record)

cur.close()
conn.close()

end = time.time()

pr_cost = end - start

print("pgRouting reading spent %f seconds" % pr_cost)
