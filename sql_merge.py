# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 16:12:38 2018      ### The scrubing was inconsistent over time, 2000-2011 some of the most extreme 2000 ticks were removedwith the 80% algorithm 
                                        ### However past that .... 
                                            
@author: ivaimberg
"""

import h5py
import pandas as pd
import pymysql
#from sqlalchemy import create_engine
import time
import numpy as np

table = 'currency_data'

data_file = 'all_year_data_scrubbed2.h5'

with h5py.File(data_file, 'r') as f:
    thing = list(f.keys())   
    thing = [x for x in thing]
    

keys = sorted(thing)
#print(keys)
"""
all_keys = [[] for x in range(19)]



for key in keys:
    try:
        year = pd.read_hdf(data_file, str(key)).iloc[0][2].year
        ones = year%2000
        all_keys[ones].append(key)
        print(all_keys)
    except:
        pass

for x in range(19):
    all_keys[x] = sorted(map(int, all_keys[x]))
    #all keys for different years are organized 
    
"""




def row_data(x, dataframe):
    return (dataframe.iloc[x][1:])


    

DB_NAME = 'EUR_USD_data'
#table_name = '`data_2018`'
table_names = ["data_"+str(2000+x) for x in range(18)]
print(table_names)
    
for x in range(18): #big loop for each year 
    print(table_names[x])
    TABLES = {}
    TABLES[table_names[x]] = ("CREATE TABLE "+table_names[x]+" (" 
          "`LTid` INT NOT NULL AUTO_INCREMENT PRIMARY KEY," 
          "`CurrencyPair` VARCHAR(7) NOT NULL,"
          "`RateDateTime` DATETIME NOT NULL," 
          "`RateBid` Float NOT NULL," 
          "`RateAsk` Float NOT NULL," 
          "`cDealable` CHAR(1) NOT NULL "
          ")")
    
 
    """
    TABLES['EUR_USDa'] = ("CREATE TABLE `EUR_USDa` (" #shitty old mistake, but afraid to delete 
          "`LTid` VARCHAR(70) NOT NULL ," 
          "`CurrencyPair` VARCHAR(7) NOT NULL,"
          "`RateDateTime` VARCHAR(7) NOT NULL," 
          "`RateBid` Float NOT NULL," 
          "`RateAsk` Float NOT NULL," 
          "`cDealable` CHAR(1) NOT NULL "
          ")")
    """
    
    add_row = ("INSERT INTO "+table_names[x]+" "
               "( `CurrencyPair`, `RateDateTime`, `RateBid`, `RateAsk`, `cDealable`) "
               "VALUES (%s,%s,%s,%s,%s)")
    
    
    
    
    
    
    
    cnn = pymysql.connect(host='localhost', user='root', password='Ivaim1997%', db='EUR_USD_data' , charset='utf8mb4')
    #engine = create_engine("mysql+pymysql://root:Ivaim1997%@localhost/EUR_USD_data")
    cursor = cnn.cursor()
    #cnn = engine.connect()



    cursor.execute(TABLES[table_names[x]])
    chunk_size = 1000
    for key in all_keys[x]:
        print(key)
        data = pd.read_hdf(data_file, str(key))
        table_length = len(data)
        last_list_length = len(data)%chunk_size             # this whole process uses multi-insert via the executemany method
                                                            # this code used to organize values for importing them as tuples of strings not original data types in pandas 
    #for x in range(len(data)):
    
        for y in np.arange(0,table_length-last_list_length,chunk_size):
            z = [tuple(map(str,list(data.iloc[x][1:]))) for x in [y+q for q in range(chunk_size)]]
        #a = [x for x in [y+q for q in range(10)]]
        #print(a)
        #cursor.execute(add_row, row_data(x,data))
            #start = time.time()
            
            cursor.executemany(add_row, z)
            cnn.commit()
            #end = time.time()
           # print((end-start)/60)
    
        last_indexes = [len(data)-x-1 for x in range(last_list_length)]
        last_rows_data = [tuple(map(str,list(data.iloc[t][1:]))) for t in last_indexes[:-1:-1]]
        #start = time.time()
        cursor.executemany(add_row, last_rows_data)
        cnn.commit()
        #end = time.time()
        #print((end-start)/60)
        #print(last_indexes[::-1])
           
        
    #q = tuple(map(str,list(data.iloc[0][1:])))
    #print(add_row_2 % ('1','2','3','4','5','6'))
    #cursor.execute(add_row, q)
        
    #cursor.execute(add_row, ('z','b','c','1.0','1.0','f'))
    cnn.commit()
    cursor.close()
    cnn.close()
    #end = time.time()
    



