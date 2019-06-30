#################
#  Projet ASOS  #
#    TD2-5-6    #
# Guyot - Zhang #
#################

from cassandra.cluster import Cluster
import numpy as np
import math
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime

import findspark
findspark.init('/opt/spark')
from pyspark import SparkContext
from pyspark import SparkConf



# KEYSPACE #
KEYSPACE = "claire_haifei_projet"



# Create day-month-year string #
def toYMD(year,month,day):
    YMD = str(year)
    if month<10:
        YMD = YMD+'/0'+str(month)
    else:
        YMD = YMD+'/'+str(month)
    if day<10:
        YMD = YMD+'/0'+str(day) 
    else:
        YMD = YMD+'/'+str(day)
    return YMD


# Create max temperature per day plot #
def plotDailyMaxtemp(daily_max_min_temp,station,year):
    time = [datetime.strptime(d, '%Y/%m/%d').date() for d in daily_max_min_temp[:,0]]
    plt.figure(figsize=(20,10))
    plt.plot(time,daily_max_min_temp[:,1], '-',linewidth=1)
    plt.title(f'''Température max de chaque jour en {year} de {station}''')
    plt.xlabel("Jour")
    plt.ylabel("Température max")
    plt.legend()
    plt.savefig("images/Temperature_max_de_chaque_jour_en_{0}_de_{1}.png".format(year,station))


# Create average temperature per quarter plot #
def plotTemperatureMoyenneMensuel(moyen_temperature,station):
    plt.figure(figsize=(8,4))
    plt.plot(moyen_temperature[moyen_temperature[:,0]==2011,1:3][:,0],moyen_temperature[moyen_temperature[:,0]==2011,1:3][:,1], color='g', marker= 'o',linewidth=1,label='2011')
    plt.plot(moyen_temperature[moyen_temperature[:,0]==2012,1:3][:,0],moyen_temperature[moyen_temperature[:,0]==2012,1:3][:,1], color='b', marker= 'o',linewidth=1,label='2012')
    plt.plot(moyen_temperature[moyen_temperature[:,0]==2013,1:3][:,0],moyen_temperature[moyen_temperature[:,0]==2013,1:3][:,1], color='r', marker= 'o',linewidth=1,label='2013')
    plt.plot(moyen_temperature[moyen_temperature[:,0]==2014,1:3][:,0],moyen_temperature[moyen_temperature[:,0]==2014,1:3][:,1], color='y', marker= 'o',linewidth=1,label='2014')
    plt.title(f'''Température moyenne trimestrielle de {station}''')
    plt.xlabel("Saison")
    plt.ylabel("Température moyenne")
    plt.legend()
    plt.savefig("images/Temperature_moyenne_trimestrielle_{0}.png".format(station))


# Create max-min temperature per month plot #
def plotTemperatureMaxMinTri(max_min_temp,station):
    plt.figure(figsize=(10,6))
    plt.plot(max_min_temp[max_min_temp[:,0]==2011,1:4][:,0],max_min_temp[max_min_temp[:,0]==2011,1:4][:,1], color='g', marker= 'o',linewidth=1,label='2011 max')
    plt.plot(max_min_temp[max_min_temp[:,0]==2012,1:4][:,0],max_min_temp[max_min_temp[:,0]==2012,1:4][:,1], color='b', marker= 'o',linewidth=1,label='2012 max')
    plt.plot(max_min_temp[max_min_temp[:,0]==2013,1:4][:,0],max_min_temp[max_min_temp[:,0]==2013,1:4][:,1], color='r', marker= 'o',linewidth=1,label='2013 max')
    plt.plot(max_min_temp[max_min_temp[:,0]==2014,1:4][:,0],max_min_temp[max_min_temp[:,0]==2014,1:4][:,1], color='y', marker= 'o',linewidth=1,label='2014 max')
    plt.plot(max_min_temp[max_min_temp[:,0]==2011,1:4][:,0],max_min_temp[max_min_temp[:,0]==2011,1:4][:,2], color='g', marker= '*',linestyle='--',linewidth=1,label='2011 min')
    plt.plot(max_min_temp[max_min_temp[:,0]==2012,1:4][:,0],max_min_temp[max_min_temp[:,0]==2012,1:4][:,2], color='b', marker= '*',linestyle='--',linewidth=1,label='2012 min')
    plt.plot(max_min_temp[max_min_temp[:,0]==2013,1:4][:,0],max_min_temp[max_min_temp[:,0]==2013,1:4][:,2], color='r', marker= '*',linestyle='--',linewidth=1,label='2013 min')
    plt.plot(max_min_temp[max_min_temp[:,0]==2014,1:4][:,0],max_min_temp[max_min_temp[:,0]==2014,1:4][:,2], color='y', marker= '*',linestyle='--',linewidth=1,label='2014 min')
    plt.title(f'''Température max-min mensuelle de {station}''')
    plt.xlabel("Mois")
    plt.ylabel("Température max~min")
    plt.legend()
    plt.savefig("images/Temperature_max_min_mensuelle_{0}.png".format(station))


# Create wind rose plot #
def plotWindRose(wind_direction_frequency,station):
    N = 8
    plt.figure(figsize=(10,10))
    for quarter in range(1,5):
        quarter_data = wind_direction_frequency[wind_direction_frequency[:,0]==quarter,1:3]
        for i in range(1,9):
            if i not in quarter_data[:,0]:
                quarter_data = np.append(quarter_data,[[i,0]],axis=0)
                quarter_data = sorted(quarter_data,key=lambda x : x[0])
        theta = np.arange(0., 2 * np.pi,2 * np.pi / N)+2 * np.pi / N
        radii = np.array(quarter_data[:,1])
        width = np.ones(8)*2*np.pi/N
        ax = plt.subplot(2,2,quarter, projection='polar')
        bars = ax.bar(theta, radii, width=width, bottom=0.0)
        for r, bar in zip(radii, bars):
            bar.set_facecolor(plt.cm.viridis(r / 5000.))
            bar.set_alpha(0.5)
        ax.set_title(f'''Wind rose plot pour {station} tri{quarter}''')
    plt.tight_layout()
    plt.savefig(f'''images/Wind_rose_plot_{station}.png''')


##############
# QUESTION 1 #
##############

def reponseQuestion1():
    # Create spark environment
    conf=SparkConf().setAppName("PySparkShell").setMaster("local[*]")
    sc=SparkContext.getOrCreate(conf)
    # Create connexion
    cluster = Cluster(["localhost"])
    session = cluster.connect(KEYSPACE)
    
    # User input
    choix = input("""
Voulez-vous donner
     1 : un nom de station
     2 : les coordonnees d une station ?

Tapez 1 ou 2: """)

    if choix =='1':
        station = input("\nDonnez la station : ")
        query = query = "SELECT lat, lon FROM stations where station='{}' ALLOW FILTERING;".format(station)
        result = session.execute(query)
        if result.one() :
            latitude = result.one()[0]
            longitude =  result.one()[1]
        else :
            print("\n*** La station n'existe pas ! ***\n")
            return
    elif choix =='2' :
        print("\nDonnez une localisation avec latitude et longitude.\n")
        latitude_input = float(input("Donnez la latitude : "))
        longitude_input = float(input("Donnez la longitude : "))

        # Find station from key (latitude,longitude)
        query = "SELECT lat, lon FROM stations;"
        result = session.execute(query)
        D = sc.parallelize(result)
        station = np.array(D.map(lambda data : (round((data[0]-latitude_input)**2+(data[1]-longitude_input)**2,4),data[0],data[1])).distinct().collect())
        station_proche = station[np.where(station==min(station[:,0]))[0],1:3] 
        latitude = station_proche[0,0]
        longitude = station_proche[0,1]
        station = session.execute("SELECT station FROM stations WHERE lat={} AND lon={};".format(latitude,longitude)).one()[0]
    else :
        print("\n*** Mauvais choix ! ***\n")
        return 

    year = int(input("\nDonnez l annee entre 2011 et 2014 : "))
    while (year < 2011 or year > 2014):
        print("\nMauvaise annee !\n")
        year = int(input("\nDonnez l annee entre 2011 et 2014 : "))
    # Max temperature per day
    query = "SELECT year,month,day,tmp FROM asos1 WHERE lat = {} and lon = {} AND year = {} ORDER BY year,month,day ALLOW FILTERING;".format(latitude,longitude,year)
    result = session.execute(query)
    D = sc.parallelize(result)
    daily_max_min_temp = D.map(lambda data:((toYMD(data[0],data[1],data[2])),[data[3],data[3]])).reduceByKey(lambda a,b:[max(a[0],b[0]),min(a[1],b[1])]).map(lambda r:[r[0],round(r[1][0],2),round(r[1][1],2)]).collect()
    daily_max_min_temp = sorted(daily_max_min_temp,key=lambda x : x[0])
    daily_max_min_temp = np.array(daily_max_min_temp)

    # Average temperature per quarter
    query = "SELECT year,month,tmp FROM asos1 WHERE  lat = {} AND lon = {} ORDER BY year,month;".format(latitude,longitude)
    result = session.execute(query)
    D = sc.parallelize(result)
    moyen_temperature = D.map(lambda data:[(data[0],math.ceil(data[1]/3)) ,np.array([1,data[2]])]).reduceByKey(lambda a,b:a+b).map(lambda r:(r[0][0],r[0][1],round(r[1][1]/r[1][0],2))).collect()
    moyen_temperature = sorted(moyen_temperature,key=lambda moy : moy[0:2])
    moyen_temperature = np.array(moyen_temperature)

    # Max-min temperature per month
    max_min_temp = D.map(lambda data:((data[0],data[1]),[data[2],data[2]])).reduceByKey(lambda a,b:[max(a[0],b[0]),min(a[1],b[1])]).map(lambda r:[r[0][0],r[0][1],r[1][0],r[1][1]]).collect()
    max_min_temp = sorted(max_min_temp,key=lambda x : x[0:2])
    max_min_temp = np.array(max_min_temp)

    # Wind rose 
    query = "SELECT month, drct FROM asos1 WHERE lat = {} AND lon = {};".format(latitude,longitude)
    result = session.execute(query)
    D = sc.parallelize(result)
    #wind_direction_frequency = D.map(lambda data:[math.ceil(data[0]/45),1]).reduceByKey(lambda a,b:a+b).map(lambda r:[r[0],r[1]]).collect()
    wind_direction_frequency = D.map(lambda data:[(math.ceil(data[0]/3), 8 if data[1]==0 else math.ceil(data[1]/45)),1]).reduceByKey(lambda a,b:a+b).map(lambda r:[r[0][0],r[0][1],r[1]]).collect()
    wind_direction_frequency = sorted(wind_direction_frequency,key=lambda x : x[0:2])
    wind_direction_frequency = np.array(wind_direction_frequency)
    
    # Temperature boxplot
    '''
    boxdata = []
    for i in range(1,13):
        if i not in temperature[:,0]:
            continue
        else:
            boxdata.append(temperature[temperature[:,0]==i,1])
    labels = range(1,13)
    bplot = plt.boxplot(boxdata, patch_artist=True, labels=labels)
    plt.title('Température box plot')
    colors = ['dodgerblue', 'dodgerblue', 'dodgerblue', 'orange','orange','orange','orangered','orangered','orangered','deepskyblue','deepskyblue','deepskyblue']
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)  # 为不同的箱型图填充不同的颜色
    plt.xlabel('Mois')
    plt.ylabel('Température')
    plt.savefig("images/Temperature_box_plot.png")
    '''
    # Plot for max temperature per day
    plotDailyMaxtemp(daily_max_min_temp,station,year)

    # Plot for average temperature per quarter
    plotTemperatureMoyenneMensuel(moyen_temperature,station)
    
    # Plot for max-min temperature per month
    plotTemperatureMaxMinTri(max_min_temp,station)

    # Plot for wind rose
    plotWindRose(wind_direction_frequency,station)

    print("""
     *** Courbes creees avec succes ! ***\n
             ==================\n""")


# Main function
if __name__ == '__main__':
    reponseQuestion1()