import json
import time
import requests
from kafka import KafkaProducer

# To do: Créer via l'invite de command un topic velib_source_datastreaming
# To do: Envoyer les données vers un topic velib_source_datastreaming
# To do: Il faut filtrer les données collectées pour ne prendre que les données des deux stations suivantes : 16107, 32017

def get_velib_data():
    """
    Get velib data from api
    :return: list of stations information
    """
    response = requests.get('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json')
    data = json.loads(response.text)
    #print(data)
    
    # Filtrer les données pour ne prendre que les stations 16107 et 32017
    filtered_stations = [station for station in data["data"]["stations"] if station["stationCode"].isdigit() and int(station["stationCode"]) in [16107, 32017]]
    print(filtered_stations)
    return filtered_stations

def velib_producer():
    """
    Create a producer to write velib data in kafka
    :return:
    """
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    while True:
        data = get_velib_data()
        for message in data:
            # Envoyer les données vers le topic velib_source_datastreaming
            producer.send("velib-projet", value=message)
            print("added:", message)
        time.sleep(1)

if __name__ == '__main__':
    velib_producer()
