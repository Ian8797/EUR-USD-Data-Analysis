# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 11:03:35 2018

@author: ivaimberg
"""

def pnl_calc(max_value, max_index, data, length):
    return -1*(max_value-data.iloc[max_index+length]["Mid"])


def max_inform(data):
     
    index = data['Mid'].idxmax()
    max_in = data.loc[index]
   
    return index, max_in["RateDateTime"], max_in["Mid"]


b1, b2, b3,b4, b5=0,0,0,0,0

import h5py 
import pandas as pd
import numpy as np
#import multiprocessing as m '
#from multiprocessing import Pool
import time 


#import time
mid_data = 'mid_2nd_scrub.h5'  
pnl_max = 'max_pnl.h5'
#pnl_max = 'max_pnl.h5' 
with h5py.File(mid_data, 'r') as f:
    thing = list(f.keys())   
    thing = [int(x) for x in thing]
  
keys = sorted(thing)

st = time.time() 

def pnl_algo(key):  
    print(keys[key])
    max_data = []
    
    #old_max_value = -1
    current_data = pd.read_hdf(mid_data, str(keys[key])).reset_index(drop=True)
    
    
    max_in = max_inform(current_data.iloc[0:1000]) ##first max recorded and added to the the max data
    
    """
    M_100 = pnl_calc(max_in[2], max_in[0], current_data, 100)
    M_500 = pnl_calc(max_in[2], max_in[0], current_data, 500)
    M_1000 = pnl_calc(max_in[2], max_in[0], current_data, 1000)
    """
    
    M_250 = pnl_calc(max_in[2], max_in[0], current_data, 250)
    M_750 = pnl_calc(max_in[2], max_in[0], current_data, 750)
    M_2000 = pnl_calc(max_in[2], max_in[0], current_data, 2000)
    M_5000 = pnl_calc(max_in[2], max_in[0], current_data, 5000)
    M_7000 = pnl_calc(max_in[2], max_in[0], current_data, 7000)
    M_10000 = pnl_calc(max_in[2], max_in[0], current_data, 10000)
    M_12000 = pnl_calc(max_in[2], max_in[0], current_data, 12000)
    M_15000 = pnl_calc(max_in[2], max_in[0], current_data, 15000)
    
    #new_row = np.array([max_in[0], max_in[2], M_100, M_500, M_1000])
    new_row = np.array([max_in[0], max_in[2], M_250, M_750, M_2000, M_5000, M_7000, M_10000, M_12000, M_15000])

    
    max_data.append(new_row)
    
    #current2 = pd.read_hdf(mid_data, str(keys[key+1])).reset_index(drop=True)
 
   
    if keys[key] != keys[-1]:
        #next_data = pd.read_hdf(mid_data, str(keys[key+1])).reset_index(drop=True).iloc[:2000]
        next_data = pd.read_hdf(mid_data, str(keys[key+1])).reset_index(drop=True).iloc[:16000]
    else:
        next_data = pd.DataFrame([])
    #frames.append(next_data)    
    #frames = [current_data, current2, current3, current4, current5, next_data]
    frames = [current_data, next_data]
    data = pd.concat(frames)
    
    data = data.reset_index(drop=True)
    #print(data)
    
    last_index = len(data.index)-1 
    
    t = 0
    #y = 999- max_in[0]
    max_value = max_in[2]
    #indices = []
    #print(last_index)
    
    
    z1 = data["Mid"]
    """
    z2 = data["Mid"].iloc[100:]
    z3 = data["Mid"].iloc[500:]
    z4 = data["Mid"].iloc[1000:]
    """    
    z2 = data["Mid"].iloc[250:]
    z3 = data["Mid"].iloc[750:]
    z4 = data["Mid"].iloc[2000:]
    z5 = data["Mid"].iloc[5000:]
    z6 = data["Mid"].iloc[7000:]
    z7 = data["Mid"].iloc[10000:]
    z8 = data["Mid"].iloc[12000:]
    z9 = data["Mid"].iloc[15000:]
    
    
       
    #for i,x in enumerate(zip(z1,z2,z3,z4)):
    for i,x in enumerate(zip(z1,z2,z3,z4,z5,z6,z7,z8,z9)):   
        if i > max_in[0]:
            if i <= last_index-15001:
               
                if t < 999:
                    
                    if max_value < x[0]:
                        
                        #indices.append(i) #needed to convert some indices to times 
                        max_value = x[0]
                        
                       
                        M_250 = -1*(max_value-x[1])
                        M_750 = -1*(max_value-x[2])
                        M_2000 = -1*(max_value-x[3])
                        M_5000 = -1*(max_value-x[4])
                        M_7000 = -1*(max_value-x[5])
                        M_10000 = -1*(max_value-x[6])
                        M_12000 = -1*(max_value-x[7])
                        M_15000 = -1*(max_value-x[8])
                      
                        new_row = np.array([i, max_value, M_250, M_750, M_2000, M_5000, M_7000, M_10000, M_12000, M_15000])
                        
                        #new_row = np.array([i, max_value, M_100, M_500, M_1000])
                        max_data.append(new_row)
                        t = 0 
                          
                    else:
                         t+=1 
                        
                        
                    
                else:
                   # print(i,t,max_in[0])
                    max_value = -1
                    #print(data["Mid"].iloc[i-999:i+1])
                    for it, y in enumerate(zip(data["Mid"].iloc[i-999:i+1])):
                        if y[0] > max_value:
                             max_value = y[0]
                             index = it+i-999 
                    
                    for it_2, y in enumerate(zip(data["Mid"].iloc[index:index+15001])):
                        
                        if it_2 == 250:
                            M_250 = (max_value - y[0])*-1
                        if it_2 == 750:
                            M_750 = (max_value - y[0])*-1
                        if it_2 == 2000:
                            M_2000 = (max_value - y[0])*-1
                        if it_2 == 5000:
                            M_5000 = (max_value - y[0])*-1
                        if it_2 == 7000:
                            M_7000 = (max_value - y[0])*-1
                        if it_2 == 10000:
                            M_10000 = (max_value - y[0])*-1
                        if it_2 == 12000:
                            M_12000 = (max_value - y[0])*-1   
                        if it_2 == 15000:
                            M_15000 = (max_value - y[0])*-1 
                         
                    new_row = np.array([index, max_value, M_250, M_750, M_2000, M_5000, M_7000, M_10000, M_12000, M_15000])
                    #print(new_row)  
                    max_data.append(new_row)
                    t = 0 
                    
            else:
                pass
        else:
            pass
    
        

    num = pd.DataFrame(max_data)
    num[0] = data.loc[num[0]]["RateDateTime"].reset_index(drop=True)
    
    
    old_pnl = pd.read_hdf('max_pnl.h5',str(keys[key]))
    #print(old_pnl,num)
    for x in range(8):
        old_pnl[x+5] = num[x+2]
        
    old_pnl.to_hdf('max_pnl_ext.h5', str(key), format='table', append=True)
    #num.to_hdf('max_pnl.h5', str(key), format='table', append=True)        

"""   
if __name__=='__main__':
    __spec__ = None
    arr = [x for x in range(800)]
    p = m.Pool(4)
    p.map_async(pnl_algo, arr)
   
    p.close()
    p.join() 
"""   
"""
if __name__=='__main__':
    __spec__ = None
    arr = [2*x for x in range(404)]
    p = Pool(processes=5)
    p.map(pnl_algo, arr)
"""   
#for x in [0]:
for x in range(len(keys)):
    pnl_algo(x)            
       

et = time.time()
print(et-st)
#print(pd.DataFrame(max_data))