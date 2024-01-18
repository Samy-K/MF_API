# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 18:07:58 2024

@author: Samy-K
"""

import pandas as pd

class DatasetManager:
    def __init__(self, data):
        """
        Initialize the DatasetManager with a pandas DataFrame.
        """
        self.data = data

    @classmethod
    def from_csv(cls, file_path, separator=';', decimal=','):
        """
        Class method to create an instance of DatasetManager from a CSV file.
        """
        data = pd.read_csv(file_path, sep=separator, decimal=decimal)
        return cls(data)

    def check_quality(self):
        """
        Check for missing or inconsistent data and calculate the percentage of missing values per column.
        """
        total_rows         = self.data.shape[0]
        missing_data       = self.data.isnull().sum()
        missing_percentage = (missing_data / total_rows) * 100
        return missing_percentage

    def list_parameters(self):
        """
        List all the parameters (columns) in the dataset.
        """
        return self.data.columns.tolist()

    def basic_statistics(self):
        """
        Provide basic statistical insights about the dataset.
        """
        return self.data.describe()

    def data_summary(self):
        """
        Display a summary of the dataset including types of data and sample values.
        """
        return {
            "Data Types": self.data.dtypes,
            "First Five Rows": self.data.head()
        }

    def create_subset(self, columns=None):
        """
        Creates a subset of the data based on the provided column names.
        The subset is returned as a new DatasetManager instance.
        """
        default_columns = ['DATE', 'PSTAT', 'T', 'UABS', 'U', 'TD', 'GLO', 'DIR', 'DIF', 'N', 'INFRAR', 'DD', 'FF', 'RR1']
        if columns is None:
            columns = default_columns
        if not all(column in self.data.columns for column in columns):
            raise ValueError("One or more columns are not in the dataset")
        subset_data = self.data[columns]
        subset_data['DATE'] = pd.to_datetime(subset_data['DATE'], format='%Y%m%d%H')
        return DatasetManager(subset_data)
    
    def save_subset_as_csv(self, station_name, start_year, end_year, station_info):
        """
        Saves the subset as a CSV file, filename is composed with station name, start year, and end year.
        """
        file_name     = "RAW_DATA_" + station_name + '_' + str(start_year) + '-' + str(end_year) + '.csv'
        filtered_data = self.data
        filtered_data.to_csv(file_name, index=False, )

        with open(file_name, 'r') as fichier:
            contenu_original = fichier.readlines()
        with open(file_name, 'w') as fichier:
            for col in station_info.columns:
                fichier.write(f"#{col:10} : {station_info.iloc[0][col]}\n")
            fichier.writelines(contenu_original)
        
        print(f"Subset saved as {file_name}")

