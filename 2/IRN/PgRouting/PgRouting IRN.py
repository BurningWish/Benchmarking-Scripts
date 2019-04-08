import psycopg2
import time
import pickle

start = time.time()

db_name = 'IRN_NCL_PR'
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
cur.execute("select id from edges as e \
            where st_intersects(e.geom, st_geomfromtext('%s'))" % citycat_wkt)
results = cur.fetchall()
for result in results:
    flooded_eids.append(result[0])


# find all bids
bids = []
cur.execute("select bid from edges_vertices_pgr \
            where type = 'building'")
results = cur.fetchall()
for result in results:
    bids.append(result[0])

disrupted_bids = []
problem_bids = []

# find centre node
c_id = None
cur.execute("select bid from edges_vertices_pgr \
            where type = 'centre'")
results = cur.fetchall()
for result in results:
    c_id = result[0]


# find disrupted buildings
cur.execute("select * from pgr_dijkstra(\
            'select gid as id,\
            source::integer,\
            target::integer,\
            length::double precision as cost\
            from edges',\
            %d, array%s, FALSE)" % (c_id, bids))
results = cur.fetchall()
for result in results:
    end_vid = result[2]
    edge_id = result[4]
    if edge_id in flooded_eids:
        disrupted_bids.append(end_vid)
    
# remove edges
cur.execute("delete from edges where id in '%s'" % flooded_eids)
conn.commit()

# let's re do dijkstra path
ok_bids = []
cur.execute("select * from pgr_dijkstra(\
            'select gid as id,\
            source::integer,\
            target::integer,\
            length::double precision as cost\
            from edges',\
            %d, array%s, FALSE)" % (c_id, disrupted))
results = cur.fetchall()
for result in results:
    end_vid = result[2]
    if end_vid not in ok_bids:
        ok_bids.append(end_vid)

problem_bids = list(set(disrupted_bids).difference(set(ok_bids)))

conn = None

end = time.time()

cost = end - start

print("Spent %f seconds" % cost)