# Databricks notebook source
import csv
import os
from functools import reduce
import pandas as pd
import numpy as np

# COMMAND ----------


dbutils.widgets.text('input_data_path', '/dbfs/mnt/sue_wallace/inverted_index/', '/dbfs/mnt/sue_wallace/inverted_index/')

input_data_path  = '/dbfs/mnt/sue_wallace/inverted_index/'

dbutils.widgets.text('write_index_to', '/dbfs/mnt/sue_wallace/sue_wallace_csv/inverted_index.csv', '/dbfs/mnt/sue_wallace/sue_wallace_csv/inverted_index.csv')

write_index_to  = '/dbfs/mnt/sue_wallace/sue_wallace_csv/inverted_index.csv'


# COMMAND ----------

def lower_clean_str(x):
  
  """Function to clean up data by taking out puncuation marks"""
  
  punctuation ='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
  
  lowercased_str = x.lower()
  for x in punctuation:
    lowercased_str = lowercased_str.replace(x, '')
  
  return lowercased_str

# COMMAND ----------

import time
start = time.time()

def createDictionary():

    my_word_dict = {}

    for file in os.listdir(input_data_path):
      path = input_data_path+file
      path = path.replace('/dbfs','')
      doc = file.replace(input_data_path,'')
      
      #Create an RDD from every file in the file path
      RDD = sc.textFile(path)
      
      # Split the lines of baseRDD into words
      splitRDD = RDD.flatMap(lambda x: x.split())
      
      print(f'Total number of words in {doc}:', splitRDD.count())
      
      # take out punctuation and make lowercase
      clean_RDD = splitRDD.map(lower_clean_str)
      clean_RDD = clean_RDD.collect()
      
      for word in clean_RDD:
        
        if word not in my_word_dict.keys():
          
          my_word_dict[word] = {}
          my_word_dict[word]['Document'] = []
          
        if doc not in my_word_dict[word]['Document']:
          my_word_dict[word]['Document'] += [doc] 
    df = pd.DataFrame.from_dict(my_word_dict, orient="index")

    df.to_csv(write_index_to, index = True)
     
    return my_word_dict
  
my_data = createDictionary()

end = time.time()
time = round(end - start)
print(f'Inverted index took {time} seconds to run')
