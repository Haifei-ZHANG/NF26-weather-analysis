#################
#  Projet ASOS  #
#    TD2-5-6    #
# Guyot - Zhang #
#################

from cassandra.cluster import Cluster
import pandas as pd
import numpy as np
import folium   
from folium.plugins import HeatMap 



# KEYSPACE #
KEYSPACE = "claire_haifei_projet"



##############
# QUESTION 2 #
##############

def reponseQuestion2():
    # Create connexion
    cluster = Cluster(["localhost"])
    session = cluster.connect(KEYSPACE)
    
    # User input
    instant = input("\nDonnez un instant (par example 2014-6-15 12:55) : ")
    qury = f'''SELECT station, lon, lat, tmp, relh, drct, sknt, vsby FROM asos2 WHERE time='{instant}';'''
    result = session.execute(qury)
    map_data = pd.DataFrame()
    for row in result:
        map_data = map_data.append([[element for element in row]], ignore_index=True)
    map_data.columns=['station','lon','lat','temperature','relative_humidity','wind_direction','wind_speed','visibility']
    
    # Create point map
    weather_map = folium.Map(location=[42, 13], zoom_start=6)
    for i in range(len(map_data)):
        # Define popup
        popup_text = folium.Html(
                        '<b>Temps d\'observation : {}</b></br> <b>Station : {}</b></br> <b>lon : {}</b></br> <b>lat : {}</b></br> <b>Température (fahrenheit) : {}</b></br> \
                         <b>Humidité relative (%) : {}</b></br> <b>Direction du vent : {}</b></br> <b>Vitesse du vent (mille marin) : {}</b></br> <b>Visibilité (mile) : {}</b></br>'\
                        .format(instant,
                                map_data.iloc[i]['station'],
                                round(map_data.iloc[i]['lon'],2),
                                round(map_data.iloc[i]['lat'],2),
                                round(map_data.iloc[i]['temperature'],2),
                                round(map_data.iloc[i]['relative_humidity'],2),
                                map_data.iloc[i]['wind_direction'],
                                map_data.iloc[i]['wind_speed'],
                                 round(map_data.iloc[i]['visibility'],2)),
                        script=True)
        map_popup = folium.Popup(popup_text, max_width=2650)
        # Add points to the map
        folium.Marker([map_data.iloc[i]['lat'], map_data.iloc[i]['lon']], popup=map_popup).add_to(weather_map)
    # Save map
    weather_map.save(f'''images/Meteo_carte_{instant}.html''')

    # Create heat map
    lats = np.array(map_data['lat'][0:len(map_data)])
    lons = np.array(map_data['lon'][0:len(map_data)])
    tmps = np.array(map_data['temperature'][0:len(map_data)])
    heat_data = [[lats[i],lons[i],tmps[i]] for i in range(len(map_data))]
    heat_map = folium.Map(location=[42, 13], zoom_start=6)
    HeatMap(heat_data).add_to(heat_map)
    for i in range(len(map_data)):
        # Define popup
        popup_text = folium.Html(
                        '<b>Station : {}</b></br> <b>lon : {}</b></br> <b>lat : {}</b></br> <b>Température (fahrenheit) : {}</b>'\
                        .format(map_data.iloc[i]['station'],
                                round(map_data.iloc[i]['lon'],2),
                                round(map_data.iloc[i]['lat'],2),
                                round(map_data.iloc[i]['temperature'],2)),
                        script=True)
        map_popup = folium.Popup(popup_text, max_width=2650)
        # Add points to the map
        folium.Marker([map_data.iloc[i]['lat'], map_data.iloc[i]['lon']], popup=map_popup).add_to(heat_map)
    heat_map.save(f'''images/Carte_de_chaleur_{instant}.html''')

    print("""
     *** Cartes creees avec succes ! ***\n
             ==================\n""")

# Main function
if __name__ == '__main__':
    reponseQuestion2()