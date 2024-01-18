# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 17:07:52 2024

@author: Samy-K
"""

import requests
import time
import pandas as pd

TOKEN_URL    = "https://portail-api.meteofrance.fr/token"

class Client(object):

    def __init__(self, api_key=None, application_id=None, base_url=None):
        self.base_url = base_url
        self.session  = requests.Session()
        self.api_key  = api_key
        self.application_id = application_id
        if api_key:  
            self.session.headers.update({'apikey': self.api_key})
        elif application_id:  
            self.obtain_oauth2_token()

    def request(self, method, url, **kwargs):
        response = self.session.request(method, url, **kwargs)
        if response.status_code == 401:  
            if self.application_id:
                self.obtain_oauth2_token()
                response = self.session.request(method, url, **kwargs)
            else:
                print('The application ID has not been supplied and the token is out of date. Please modify API_config.txt with the correct credentials.')
        return response

    def obtain_oauth2_token(self):
        data = {'grant_type': 'client_credentials'}
        headers = {'Authorization': 'Basic ' + self.application_id}
        access_token_response = requests.post(TOKEN_URL, data=data, headers=headers)
        token = access_token_response.json()['access_token']
        self.session.headers.update({'Authorization': 'Bearer %s' % token})

    def get_stations_list(self, DEPARTEMENT):
        """
        Get the list of time stations for the desired department.
        """
        stations_url = self.base_url + "/liste-stations/horaire?id-departement=" + str(DEPARTEMENT)  
        response = self.request('GET', stations_url)
        if response.status_code == 200:
            return response.json()  
        else:
            print(f"get_stations_list() : Unexpected status code {response.status_code}: {response.text}")
            return None

    def select_station(self, stations):
        """Allows the user to select a weather station."""
        print("Stations available for the department (ID : NAME) :")
        for station in stations:
            print(f"{station['id']} : {station['nom']}")
        selected_id = input("\nEnter wanted station ID : ")
        return selected_id

    def get_station_info(self, station_id):
        """
        Get information about the selected station.
        """
        station_info_url = self.base_url + "/information-station?id-station=" + str(station_id)  
        response = self.request('GET', station_info_url)
        if response.status_code == 200:
            data = response.json() 
            # JSON to DataFrame
            ids, noms, lieuxDits, bassins, datesDebut, datesFin, types, altitudes, latitudes, longitudes = [], [], [], [], [], [], [], [], [], []
            for item in data:
                # General informations
                ids.append(item.get('id'))
                noms.append(item.get('nom'))
                lieuxDits.append(item.get('lieuDit'))
                bassins.append(item.get('bassin'))
                datesDebut.append(item.get('dateDebut'))
                datesFin.append(item.get('dateFin'))
                if 'typesPoste' in item and item['typesPoste']:
                    types.append(item['typesPoste'][0].get('type'))
                else:
                    types.append(None)
                if 'positions' in item and item['positions']:
                    position = item['positions'][0]  
                    altitudes.append(position.get('altitude'))
                    latitudes.append(position.get('latitude'))
                    longitudes.append(position.get('longitude'))
                else:
                    altitudes.append(None)
                    latitudes.append(None)
                    longitudes.append(None)
                df = pd.DataFrame({
                    'ID': ids,
                    'Nom': noms,
                    'LieuDit': lieuxDits,
                    'Bassin': bassins,
                    'DateDebut': datesDebut,
                    'DateFin': datesFin,
                    'Type': types,
                    'Altitude': altitudes,
                    'Latitude': latitudes,
                    'Longitude': longitudes
                })
            return df
        else:
            return None

    def order_station_data(self, station_id, debut, fin):
        """
        Sends a request to order hourly data from a weather station for a given period. If the request is accepted, it returns the order ID.
        
        :param station_id: The identifier of the weather station for which data is requested.
        :param debut: The start year of the period for which data is requested (format 'YYYY').
        :param end: The end year of the period for which data is requested (format 'YYYY').
        :return: The order identifier if the request is accepted, otherwise None.
    
        """
        order_url = self.base_url + f"/commande-station/horaire?id-station={station_id}&date-deb-periode={debut}-01-01T00%3A00%3A00Z&date-fin-periode={fin}-12-31T23%3A00%3A00Z"
        response  = self.request('GET', order_url)
        print("Placing order ...")
        if response.status_code == 202:
            try:
                response_json = response.json()
                order_id = response_json['elaboreProduitAvecDemandeResponse']['return']
                return order_id  
            except (ValueError, KeyError):
                print("Error extracting 'order_id' from JSON response.")
        else:
            print(f"Unexpected status code {response.status_code}: {response.text}")
            return None

    def download_command_file(self, order_id):
        """
        Attempts to download a CSV file from an asynchronous command, checking periodically if the file is ready. If a 500 error is encountered, the function retries after a delay.
        
        :param order_id: The identifier of the order for which to download the file.
        """
        url          = f"{self.base_url}/commande/fichier?id-cmde={order_id}"
        ready        = False
        attempt      = 0
        max_attempts = 10

        while not ready:
            response = self.request('GET', url)
            if response.status_code == 201:
                filename = f"command_{order_id}_RAW_DATA.csv"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"File downloaded and saved as {filename}")
                ready = True
            elif response.status_code == 204:
                print("Command is still being processed. Will check again in 10 seconds.")
                time.sleep(10)
            elif response.status_code == 404:
                print("Download failed with status code 404: The command number does not exist.")
                break
            elif response.status_code == 410:
                print("Download failed with status code 410: Production has already been delivered.")
                break
            elif response.status_code == 500:
                attempt += 1
                print(f"Attempt {attempt} - Download failed with status code 500: Production finished, failed. Will retry in 60 seconds.")
                time.sleep(60)
            elif response.status_code == 507:
                print("Download failed with status code 507: Production rejected by the system (too voluminous).")
                break
            else:
                print(f"Unexpected status code {response.status_code}: {response.text}")
                break
        if attempt == max_attempts:
            print("Maximum retry attempts reached. Please check your request or contact API support.")
        
