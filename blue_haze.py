from pyhive import hive
from sentence_transformers import SentenceTransformer, util
from datetime import datetime

import os
import pandas as pd
import numpy as np
import fastplot
import json
import re
import collections
import math
import gensim
import random
import torch
import time

class DuplicateManipulation():
    def __init__(self):
        # Initialize SentenceTransformer model
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def identify_duplicates(self, dataframe, groupby_parameters, text_column, threshold=0.9, date_col_name=""):
        # Group dataframe by specified parameters and aggregate text into lists
        grouped_df = dataframe.groupby(groupby_parameters).aggregate(list)
        tuple_count_dictionary = {}
        
        # Filter groups with at least 2 texts
        gdf = grouped_df[grouped_df[text_column].apply(lambda x: len(x) >= 2)].reset_index(0)
        # Apply find_index method to each row to identify duplicates
        potential_duplicates = gdf[[text_column,'id', 'x.data_abertura']].apply(lambda x: self.find_index(x, tuple_count_dictionary, threshold), axis=1)

        remove_list = []
        if len(potential_duplicates):
            # Select items to remove based on identified duplicates
            remove_list = self._select_items_to_remove(potential_duplicates, tuple_count_dictionary)
        
        treated_duplicates = [x[0] for x in potential_duplicates]
        return remove_list, potential_duplicates
    
    
    def _select_items_to_remove(self, duplicate_list, tuple_count_dictionary):
        remove_list = []
        for target_tuple in duplicate_list:
            for a, b in target_tuple:
                # Ensure keys exist in the dictionary before accessing them
                if a not in tuple_count_dictionary:
                    tuple_count_dictionary[a] = 0
                if b not in tuple_count_dictionary:
                    tuple_count_dictionary[b] = 0
                
                if tuple_count_dictionary[a] >= tuple_count_dictionary[b]:
                    tuple_count_dictionary[a] -= 1
                    remove_list.append(a)
                else:
                    tuple_count_dictionary[b] -= 1
                    remove_list.append(b)
    
        return remove_list
    
    def find_index(self, row_values, count_dict={}, threshold=0.9):
        text, ids, dates = row_values[0], row_values[1], row_values[2]
        text = [str(x) for x in text]

        # Utilize paraphrase mining to find duplicates
        paraphrase = util.paraphrase_mining(self.model, text)
        
        print(paraphrase)
        # Filter duplicates based on threshold
        duplicates = [(ids[x[1]], ids[x[2]]) for x in paraphrase if float(x[0]) >= threshold and  
                        self.are_dates_at_most_one_month_apart(dates[x[1]], dates[x[2]])]
        
        # Update count dictionary if duplicates are found
        if len(duplicates) != 0:
            self._apply_dict(count_dict, duplicates)
        
        return duplicates
    
    def are_dates_at_most_one_month_apart(self, date_str1, date_str2):
        # Convert date strings to datetime objects
        date1 = datetime.strptime(date_str1, '%Y-%m-%d')
        date2 = datetime.strptime(date_str2, '%Y-%m-%d')

        # Calculate the difference in days
        difference = abs((date2 - date1).days)

        # Check if the difference is at most 31 days
        return difference <= 31
    
    def _apply_dict(self, cdict, tuplez):
        for item in tuplez:
            for tt in [item[0], item[1]]:
                if tt not in cdict:
                    cdict[tt] = 1
                else:
                    cdict[tt] += 1


def getCursor(username, password):
    conn = hive.Connection(host="hadoopmn-gsi-prod04.mpmg.mp.br", 
    port=10500, auth='CUSTOM', 
    username=username, 
    password=password)
    return conn


def main():
    cursor = getCursor('ufmg.maraujo', 'Marcelo pesquisador123')
    
    cursor.execute('SELECT DISTINCT x.nome_reclamante_show, x.nome_reclamado_show, x.texto, x.emails_reclamante, x.data_abertura \
    FROM dataset_v2.senacon_consumidorgov x \
    WHERE desc_tipo_texto = "Descrição da reclamação" AND YEAR(data_abertura) = 2023 \
    LIMIT 200000')
    
    data = cursor.fetchall()

    # Fetch column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Create DataFrame with fetched data and column names
    df = pd.DataFrame(data, columns=columns)
    df['id'] = df.index.tolist()
    
    xoxo = DuplicateManipulation()
    
    groupby_parameters = ['x.nome_reclamante_show', 'x.nome_reclamado_show', 'x.emails_reclamante']
    reclamation_column = "x.texto"
   
    # resulterson = fobj.filter_dataframe(df, groupby_parameters, reclamation_column)
    resulterson_duplicates, all_duplicates = xoxo.identify_duplicates(df, groupby_parameters, reclamation_column)
    
    # Define the file path where you want to save the JSON file
    json_file_path = "duplicates_data.json"

    treated_duplicates_lt = [x for x in all_duplicates.tolist() if len(x)]
    
    # Prepare the data to be saved in JSON format
    data_to_save = {
        "resulterson_duplicates": resulterson_duplicates,
        "all_duplicates": treated_duplicates_lt
    }

    # Write the data to the JSON file
    with open(json_file_path, "w") as json_file:
        json.dump(data_to_save, json_file)



main()