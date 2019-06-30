import csv
from cassandra.cluster import Cluster

def loadata(filename):
    with open(filename) as f:
        for r in csv.DictReader(f):
            data = {}
            data["station"] = r["station"]
            data["lon"] = float(r["lon"]) if r["lon"]!='null' else 'null'
            data["lat"] = float(r["lat"]) if r["lat"]!='null' else 'null'
            yield data


cluster = Cluster(["localhost"])
session = cluster.connect("claire_haifei_projet")


reader = loadata('stations.csv')
query = ''
stations = []
for item in reader:
    if item['station'] not in stations:
        stations.append(item['station'])
        query = f"""
            INSERT INTO stations(lon,lat,station)values({item['lon']},{item['lat']},'{item['station']}');
            """
        session.execute(query)
    else :
        continue