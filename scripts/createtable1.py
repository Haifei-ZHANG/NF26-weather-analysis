from cassandra.cluster import Cluster

cluster = Cluster(["localhost"])
session = cluster.connect("claire_haifei_projet")

query = """
        CREATE TABLE asos1(
            year int,
            month int,
            day int,
            hour int,
            minute int,
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
        PRIMARY KEY((lat,lon),year,month,day,hour,minute));
        """
session.execute(query)