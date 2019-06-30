import csv
import re
from cassandra.cluster import Cluster

def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match_valid = dateparser.match(r["valid"])
            if not match_valid:
                continue
            valid = match_valid.groupdict()
            data = {}
            data["station"] = r["station"]
            data["valid"] = (
                int(valid["year"]),
                int(valid["month"]),
                int(valid["day"]),
                int(valid["hour"]),
                int(valid["minute"]),
            )
            data["lon"] = float(r["lon"]) if r["lon"]!='null' else 'null'
            data["lat"] = float(r["lat"]) if r["lat"]!='null' else 'null'
            data["tmp"] = float(r["tmpf"]) if r["tmpf"]!='null' else 'null'
            data["dwp"] = float(r["dwpf"]) if r["dwpf"]!='null' else 'null'
            data["relh"] = float(r["relh"]) if r["relh"]!='null' else 'null'
            data["drct"] = float(r["drct"]) if r["drct"]!='null' else 'null'
            data["sknt"] = float(r["sknt"]) if r["sknt"]!='null' else 'null'
            data["alti"] = float(r["alti"]) if r["alti"]!='null' else 'null'
            data["vsby"] = float(r["vsby"]) if r["vsby"]!='null' else 'null'
            data["skyc1"] = r["skyc1"] if r["skyc1"]!='null' else 'null'
            data["wxcodes"] = r["wxcodes"] if r["wxcodes"]!='null' else 'null'
            data["feel"] = float(r["feel"]) if r["feel"]!='null' else 'null'
            data["metar"] = r["metar"] if r["metar"]!='null' else 'null'
            yield data


cluster = Cluster(["localhost"])
session = cluster.connect("claire_haifei_projet")


reader = loadata('asos.csv')
batchFlage = 0
query = ''
for item in reader:
    if(item['tmp']=='null' or item['dwp']=='null' or item['relh']=='null' or item['drct']=='null' or item['sknt']=='null' or item['alti']=='null' or item['vsby']=='null' or item['feel']=='null'):
        continue
    else:
        batchFlage += 1
        query1 = f"""
            INSERT INTO asos1(
            lon,
            lat,
            year,
            month,
            day,
            hour,
            minute,
            station,
            tmp,
            dwp,
            relh,
            drct,
            sknt,
            alti,
            vsby,
            skyc1,
            wxcodes,
            feel)
            values(
            {item['lon']},
            {item['lat']},
            {item['valid'][0]},
            {item['valid'][1]},
            {item['valid'][2]},
            {item['valid'][3]},
            {item['valid'][4]},
            '{item['station']}',
            {item['tmp']},
            {item['dwp']},
            {item['relh']},
            {item['drct']},
            {item['sknt']},
            {item['alti']},
            {item['vsby']},
            '{item['skyc1']}',
            '{item['wxcodes']}',
            {item['feel']});
            """
        query = query+query1
        if batchFlage == 50:
            session.execute("BEGIN BATCH"+query+"APPLY BATCH")
            batchFlage = 0
            query = ''