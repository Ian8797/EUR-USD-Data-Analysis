# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 11:01:55 2018

@author: ivaimberg
"""
import requests, zipfile, io                      # !!Very Important!! Keys for the Data
import pandas as pd                               # 2000 - 2002   Key: (First,Second,Third,Fourth)_Quarter_(2000,2001,2002)
import numpy as np 
import sys                                        # January 2003  Key: January_2003
                                                  # February 2003 - Present Day Key: Week(1,2,3,4,5)_(month)_(year)
if sys.version_info[0] < 3:                 
    from StringIO import StringIO                 #####  In another file I redo the naming to make it squentially, it does from 1-943ish with about 100 wholes 
else:
    from io import StringIO  
    
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
exceptions =[]

def getzipdata(url, h5_file):  #Finds zipfiles, saves each spread sheet sequentially (Used for 2000-2002 for key naming purposes)  
        columns = ['LTid', 'CurrencyPair', 'RateDateTime', 'RateBid', 'RateAsk', 'cDealable']
        dtypes = {columns[0] : np.float , columns[1] : str , columns[2] : str, columns[3] : np.float, columns[4] : np.float, columns[5] : str }
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        with z as my_zip_file:
            for contained_file in my_zip_file.namelist():
                DataFrame = pd.read_table(z.open(contained_file), sep=',', header=0, names=columns, dtype=dtypes)
                DataFrame.to_hdf(h5_file, contained_file[21:-4], format='table', append=True)
                print(url)
                
        
def getzipdata2(url, h5_file, year):  #Finds zipfiles, saves each spread sheet sequentially  (Used for 2003-Nov_2009 for key naming purposes)
        columns = ['LTid', 'CurrencyPair', 'RateDateTime', 'RateBid', 'RateAsk', 'cDealable']
        dtypes = {columns[0] : np.float , columns[1] : str , columns[2] : str, columns[3] : np.float, columns[4] : np.float, columns[5] : str }
        try:
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            with z as my_zip_file:
                for contained_file in my_zip_file.namelist():
                   
                    DataFrame = pd.read_table(z.open(contained_file), sep=',', header=0, names=columns, dtype=dtypes)
                    DataFrame.to_hdf(h5_file, year, format='table', append=True)
                    print(url)
                
        except:
            if str(sys.exc_info()[0]) ==  str("<class 'zipfile.BadZipFile'>"):
                pass
            elif str(sys.exc_info()[0]) ==  str("<class 'UnicodeDecodeError'>"):
                r = requests.get(url)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                with z as my_zip_file:
                    for contained_file in my_zip_file.namelist():
                         raw_data = z.read(contained_file)[2::2].decode('utf-8')
                         data = StringIO(raw_data)
                         DataFrame = pd.read_csv(data, sep=',', names=columns, dtype=dtypes)
                         DataFrame.to_hdf(h5_file, year, format='table', append=True)
                         print(url)
            elif str(sys.exc_info()[0]) ==  str("<class 'ValueError'>"):
                getzipdata3(url, h5_file, year)
               
            else:
                exceptions.append([sys.exc_info()[0], year])
 
def getzipdata3(url, h5_file, year):  #Finds zipfiles, saves each spread sheet sequentially  (Used for Nov_2009-Present for key naming purposes)
        columns = ['LTid', 'cDealable', 'CurrencyPair', 'RateDateTime', 'RateBid', 'RateAsk',]
        dtypes = {columns[0] : np.float , columns[1] : str , columns[2] : str, columns[3] : str, columns[4] : np.float, columns[5] : np.float }
        try:
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            with z as my_zip_file:
                for contained_file in my_zip_file.namelist():
                    
                    bad_frame = pd.read_table(z.open(contained_file), sep=',', header=0, names=columns, dtype=dtypes)
                    DataFrame = pd.DataFrame({columns[0]: bad_frame[columns[0]], columns[2]: bad_frame[columns[2]], columns[3]: bad_frame[columns[3]], columns[4]: bad_frame[columns[4]], columns[5]: bad_frame[columns[5]], columns[1]: bad_frame[columns[1]]})
                    DataFrame.to_hdf(h5_file, year, format='table', append=True)
                    print(url)
                
        except:
            if str(sys.exc_info()[0]) ==  str("<class 'zipfile.BadZipFile'>"):
                pass
            else:
                exceptions.append([sys.exc_info()[0], year])

       
        
def urlbit(index):  #needed tool to format url dates Ex: Jan --> "01"
    
    if index < 9:
        return str(0)+str(index+1)
    else:
        return str(index+1)
    
    
data_file = 'all_year_data.h5'


for x in range(3):      # Year range is 2000-2002
    url = 'http://ratedata.gaincapital.com/'+ str(x+2000) +'/EUR_USD_'+str(x+2000)+'.zip'
    getzipdata(url, data_file)


url = 'http://ratedata.gaincapital.com/2003/01%20January/EUR_USD.zip'  #January 2003 Data
getzipdata2(url, data_file, 'January'+'_'+str(2003)) 


for x in range(16):         # Be careful so year range is correct (2003-2018) (16)
    for y in range(12):         # months range (12)
        for z in range(5): 
            url = 'http://ratedata.gaincapital.com/'+ str(x+2003) + '/' + urlbit(y) + '%20' + months[y] + '/EUR_USD_Week'+ str(z+1) +  '.zip'
            
            if x < 7:  # supposed to be (7)
                getzipdata2(url, data_file, 'Week'+str(z+1)+'_'+months[y]+'_'+str(x+2003))
                
            else:
                getzipdata3(url, data_file, 'Week'+str(z+1)+'_'+months[y]+'_'+str(x+2003))


print(exceptions)













