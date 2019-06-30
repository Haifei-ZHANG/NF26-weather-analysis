from cassandra.cluster import Cluster

cluster = Cluster(["localhost"])
session = cluster.connect("claire_haifei_projet")

query = """
        CREATE TABLE stations(
            lat float,
            lon float,
            station text,
        PRIMARY KEY((lat,lon),station));
        """
session.execute(query)