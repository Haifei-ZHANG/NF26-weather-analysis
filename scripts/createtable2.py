from cassandra.cluster import Cluster

cluster = Cluster(["localhost"])
session = cluster.connect("claire_haifei_projet")

query = """
        CREATE TABLE asos2(
            time timestamp,
            lon float,
            lat float,
            station text,
            tmp float,
            dwp float,
            relh float,
            drct float,
            sknt float,
            alti float,
            vsby float,
            skyc1 text,
            wxcodes text,
            feel float,
            metar text,
        PRIMARY KEY((time),station,lon,lat));
        """
session.execute(query)