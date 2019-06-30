#################
#  Projet ASOS  #
#    TD2-5-6    #
# Guyot - Zhang #
#################

from cassandra.cluster import Cluster
import random
import numpy as np
import pandas as pd
import folium 
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt

import findspark
findspark.init('/opt/spark')
from pyspark import SparkContext
from pyspark import SparkConf



# KEYSPACE #
KEYSPACE = "claire_haifei_projet"



# Check if k-means converges #
def converge(cluster_centers,cluster_centers_old):
    diff = np.sum((cluster_centers_old-cluster_centers)**2)
    if diff < 1e-3:
        return True
    else:
        return False


# Calculate clusters #
def calculateCluster(point,cluster_centers):
    point = np.array(point)
    distances = np.sum((point-cluster_centers)**2,axis=1).tolist()
    return distances.index(min(distances)) + 1


# K-means function #
def kMeans(data,K):
    # Create spark environment
    conf=SparkConf().setAppName("PySparkShell").setMaster("local[*]")
    sc=SparkContext.getOrCreate(conf)
    # Build RDD with data
    find_centers = sc.parallelize(data)
    # If data has no element, stop
    if len(data)==0:
        return "Aucune donnee !"
    else :
        p = len(data[0])
    # Initialize the K cluster_centers
    cluster_centers = np.array([])
    for i in range(1,K+1):
        point_index = random.randint(int((i-1)*len(data)/K),int(i*len(data)/K))
        cluster_centers = np.append(cluster_centers,data[point_index][3:p])
    cluster_centers =  cluster_centers.reshape([K,p-3])
    # Loop to converge towards cluster_centers
    while True:
        cluster_centers_old = cluster_centers
        cluster_centers = find_centers.map(lambda data:[calculateCluster(data[3:p],cluster_centers_old),np.array([1,np.array(data[3:p])])]).reduceByKey(lambda a,b:a+b).map(lambda r: r[1][1]/r[1][0]).collect()
        cluster_centers = np.array(cluster_centers)
        if(converge(cluster_centers,cluster_centers_old)):
            break
    # Prediction with converged cluster_centers
    pred = sc.parallelize(data)
    prediction = pred.map(lambda x:[x[0],x[1],x[2],calculateCluster(x[3:p],cluster_centers),x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10]]).collect()
    prediction = np.array(prediction)
    return prediction


# Create point map from clusters #
def makeMap(clusterDF,K):
    col = ['red', 'blue', 'green', 'purple', 'orange', 'pink', 'darkred','lightred', 'beige', 'darkblue', 'darkgreen', \
           'cadetblue', 'darkpurple', 'white', 'lightblue', 'lightgreen','gray', 'black', 'lightgray'][0:K+1]
    weather_map = folium.Map(location=[42, 13], zoom_start=6) 
    for i in range(len(clusterDF)):
        # Define popup
        popup_text = folium.Html(
                        '<b>Classe : {}</b></br> <b>Station : {}</b></br> <b>lon : {}</b></br> <b>lat : {}</b></br>'\
                        .format(clusterDF.iloc[i]['Class'],
                                clusterDF.iloc[i]['station'],
                                clusterDF.iloc[i]['lon'],
                                clusterDF.iloc[i]['lat']),
                                script=True)
        map_popup = folium.Popup(popup_text, max_width=2650)

        # Add point to the map
        folium.Marker([clusterDF.iloc[i]['lat'], clusterDF.iloc[i]['lon']], popup=map_popup,icon=folium.Icon(color=col[clusterDF.iloc[i]['Class']-1])).add_to(weather_map)
    # Save map
    weather_map.save("images/Clustering_carte.html")


# Create scatter plot from clusters #
def clustingScatter(clusterDF,K):
    col = ['r', 'b', 'g', 'y', 'm', 'c'][0:K+1]
    plt.figure(figsize=(8,4))
    ax = plt.subplot()
    for i in range(1,K+1):
        ax.scatter(clusterDF.loc[clusterDF.Class==i,['tmp']], clusterDF.loc[clusterDF.Class==i,['relh']], alpha=0.5, c=col[i-1])
    plt.title(f'''Résultat de clustering''')
    plt.xlabel("Température")
    plt.ylabel("Humidité relative")
    plt.savefig("images/Resultat_de_clustering")


##############
# QUESTION 3 #
##############

def reponseQuestion3():
    # Create spark environment
    conf=SparkConf().setAppName("PySparkShell").setMaster("local[*]")
    sc=SparkContext.getOrCreate(conf)
    # Create connexion
    cluster = Cluster(["localhost"])
    session = cluster.connect(KEYSPACE)
    
    # User input
    print("\nDonnez une periode avec un instant de debut et un instant de fin (par example 2014-6-15 12:55).")
    t1 = input("Debut : ")
    t2 = input("Fin : ")

    # Load data
    query = f'''SELECT station, lon, lat, tmp, relh FROM asos2 WHERE time>'{t1}' AND time<'{t2}' ALLOW FILTERING;'''
    result = session.execute(query)
    D = sc.parallelize(result)
    station_data = D.map(lambda data:[(data[0],round(data[1],2),round(data[2],2)) ,np.array([1,data[3],data[3]**2,data[3],data[3],data[4],data[4]**2,data[4],data[4]])])\
    .reduceByKey(lambda a,b:np.array([a[0]+b[0],a[1]+b[1],a[2]+b[2],min(a[3],b[3]),max(a[4],b[4]),a[5]+b[5],a[6]+b[6],min(a[7],b[7]),max(a[8],b[8])]))\
    .map(lambda r:[r[0][0],r[0][1],r[0][2],round(r[1][1]/r[1][0],2),round(np.sqrt((r[1][2]/r[1][0])-(r[1][1]/r[1][0])**2),2),round(r[1][3],2),round(r[1][4]),\
    round(r[1][5]/r[1][0],2),round(np.sqrt((r[1][6]/r[1][0])-(r[1][5]/r[1][0])**2),2),round(r[1][7],2),round(r[1][8])]).collect()   
    
    # Make k-means clustering
    cluster_result = kMeans(station_data,4)
    
    # Create dataframe from clusters for map
    clusterDF = pd.DataFrame(cluster_result)
    
    columns =['station','lon','lat','Class','tmp','tmp-std','tmp-min','tmp-max','relh','relh-std','relh-min','relh-max'] 
    clusterDF.columns = columns
    for colum in columns:
        if colum == 'station':
            continue
        else :
            clusterDF[[colum]] = clusterDF[[colum]].astype(float)
    clusterDF[['Class']] = clusterDF[['Class']].astype(int)
    
    # Create the clustering map
    makeMap(clusterDF,4)
    
    # Create the clustering scatter plot
    clustingScatter(clusterDF,4)

    print("""
   *** Clustering effectue avec succes ! ***\n
             ==================\n""")
 
#main function
if __name__ == '__main__':
    reponseQuestion3()