# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 17:07:52 2024

@author: Samy-K

Copyright 2024 Samy Kraiem

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import requests
import time
import pandas as pd

TOKEN_URL    = "https://portail-api.meteofrance.fr/token"

class Client(object):

    def __init__(self, api_key=None, application_id=None, base_url=None):
        if not api_key and not application_id:
            raise ValueError("Either an Token or an Application ID must be provided.")
        self.base_url = base_url if base_url is not None else "DEFAULT_BASE_URL"
        self.session = requests.Session()
        self.api_key = api_key
        self.application_id = application_id
        if api_key:
            self.session.headers.update({'apikey': self.api_key})
        else:
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

    def order_station_data(self, station_id, start_year, end_year):
        order_ids = []
        for year in range(int(start_year), int(end_year) + 1):
            print(f"Placing order for the year {year}...")
            order_url = self.base_url + f"/commande-station/horaire?id-station={station_id}&date-deb-periode={year}-01-01T00%3A00%3A00Z&date-fin-periode={year}-12-31T23%3A00%3A00Z"
            response = self.request('GET', order_url)
            if response.status_code == 202:
                try:
                    response_json = response.json()
                    order_id = response_json['elaboreProduitAvecDemandeResponse']['return']
                    order_ids.append(order_id)
                except (ValueError, KeyError):
                    print(f"Error extracting 'order_id' for the year {year}.")
            else:
                print(f"Unexpected status code {response.status_code} for the year {year}: {response.text}")
        return order_ids

    def download_command_file(self, order_ids):
        for order_id in order_ids:
            url = f"{self.base_url}/commande/fichier?id-cmde={order_id}"
            ready = False
            attempt = 0
            max_attempts = 10
    
            while not ready and attempt < max_attempts:
                response = self.request('GET', url)
                if response.status_code == 201:
                    filename = f"command_{order_id}_RAW_DATA.csv"
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    print(f"File for order {order_id} downloaded and saved as {filename}")
                    ready = True
                elif response.status_code in [204, 500]:
                    attempt += 1
                    wait_time = 10 if response.status_code == 204 else 60
                    print(f"Attempt {attempt} for order {order_id} - Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                elif response.status_code in [404, 410, 507]:
                    print(f"Download failed for order {order_id} with status code {response.status_code}: {response.text}")
                    break
                else:
                    print(f"Unexpected status code {response.status_code} for order {order_id}: {response.text}")
                    break
            if attempt == max_attempts:
                print(f"Maximum retry attempts reached for order {order_id}.")

       
