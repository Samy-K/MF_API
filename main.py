# -*- coding: utf-8 -*-
"""
Created onWed Jan 17 18:07:13 2024

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

import pandas as pd
from datetime import datetime
import configparser
import os
from client import *
from Data_handler import *

def main():
    
    global BASE_API_URL
    config = configparser.ConfigParser()
    config.read("API_config.txt")

    application_id = config.get("Parameters", "APPLICATION_ID")
    BASE_API_URL   = config.get("Parameters", "DATA_SERVER")
    api_key        = config.get("Parameters", "TOKEN").replace('"', '')
    print("-" * 30)
    print('Credidentials :\n')
    print(f"APPLICATION ID : {application_id}\nSERVER         : {BASE_API_URL}\nTOKEN          : {api_key[:32]}....")
    print("-" * 30)
    
    client = Client(api_key=api_key, base_url=BASE_API_URL)  
    
    # Department choice
    while (DEPARTEMENT := input("\nEnter the desired department number. Numbers range from 1 to 95, plus 971, 972, 973, 974, 975, 984, 985, 986, 987, 988 for French overseas departments and territories.\n\nDepartment :"\
                                ).strip()) not in [str(i) for i in range(1, 96)] + ['971', '972', '973', '974', '975', '976', '977', '978', '984', '986', '987', '988', '989']: pass
    
    # Get the department weather station list
    stations = client.get_stations_list(DEPARTEMENT)
    if stations:
        # Station choice
        selected_station = client.select_station(stations)
        
        # Retrieve selected station information
        station_info = client.get_station_info(selected_station)
        print("-" * 30)
        print("Selected weather station informations :\n")
        for col in station_info.columns:
            print(f"{col:10} : {station_info.iloc[0][col]}")
        print("-" * 30)
        
        # Time period selection
        annee_minimale = pd.to_datetime(station_info.iloc[0]['DateDebut']).year
        annee_maximale = pd.to_datetime(station_info.iloc[0]['DateFin'], errors='coerce').year if not \
            pd.isna(pd.to_datetime(station_info.iloc[0]['DateFin'], errors='coerce')) else datetime.now().year

        while not (annee_minimale <= (B_annee := int(input("Please enter a starting year > DateDebut (YYYY format)  : "))) < annee_maximale): pass    
        while not (B_annee        <= (E_annee := int(input("Please enter a ending year < DateFin (YYYY format)      : "))) < annee_maximale): pass
        
        # Place order
        print("-" * 30)
        print("Order summary :\n")
        for col in station_info.drop(columns=['LieuDit', 'Bassin', 'DateDebut', 'DateFin']).columns:
            print(f"{col:10}  : {station_info.iloc[0][col]}")
        print(f"Time period : {B_annee}-{E_annee}")
        print("-" * 30)
        confirmation = 'Y' if input("\nPlace Order ? (Y/n) : ").strip().lower() != 'n' else 'n'
        if confirmation == 'Y':
            order_id = client.order_station_data(selected_station, B_annee, E_annee)
            if order_id:
                print("Order placed successfully. Order ID :", order_id)
                client.download_command_file(order_id)
        else:
            print("Cancelled.")
    else:
        print("FAIL !")

    # Formatting data
    file_path     = f'command_{order_id}_RAW_DATA.csv' 
    dataset       = DatasetManager.from_csv(file_path)
    final_dataset = dataset.create_subset()
    quality_check = final_dataset.check_quality()    # Check for missing data
    parameters    = final_dataset.list_parameters()  # List all parameters
    basic_stats   = final_dataset.basic_statistics() # Basic statistical insights
    data_summary  = final_dataset.data_summary()     # Summary of the dataset
    
    print("-" * 30)
    print("Missing data per parameter (parameter / % missing) :\n")
    with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        print(quality_check)
    print("-" * 30)
    
    # Writing data
    final_dataset.save_subset_as_csv(station_name = str(station_info.iloc[0]['ID']) + '_' + str(station_info.iloc[0]['LieuDit']).replace(' ', '-'), 
                                       start_year=2020, end_year=2023, station_info=station_info)
    if os.path.exists(file_path):
        os.remove(file_path)

if __name__ == '__main__':
    main()
