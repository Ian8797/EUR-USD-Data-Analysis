# -*- coding: utf-8 -*-
"""
Created on Mon Aug 27 11:32:48 2018

@author: ivaimberg
"""

def pnl_calc(min_value, min_index, data, key, length):
    current_length = len(data.index)
    tick_needed = min_index+length
    
    if tick_needed < current_length:
        return min_value-data.iloc[min_index+length]["Mid"]
    else:
        total_length = current_length
        index_taken = tick_needed-current_length
        present_length = 0
        while tick_needed > total_length:
            key+=1
            index_taken-=present_length
            present_length += len(pd.read_hdf(all_data, str(keys[key])).index)
            total_length+=present_length
        return min_value-pd.read_hdf(mid_data, str(keys[key]))["Mid"].iloc[index_taken]



def min_inform(mid_data, all_data):
     
    index = mid_data['Mid'].idxmin()
    min_in = mid_data.loc[index]
   
    return index, min_in["RateDateTime"], min_in["Mid"]




import h5py 
import pandas as pd
import numpy as np

import time 
import sys
#lengths = []




#import time
mid_data = 'mid_2nd_scrub.h5'
all_data = 'data_2nd_scrub.h5'  
#pnl_min = 'min_pnl.h5'
#pnl_min = 'min_pnl.h5' 
with h5py.File(mid_data, 'r') as f:
    thing = list(f.keys())   
    thing = [int(x) for x in thing]
  
keys = sorted(thing)

#for key in keys: 
    #lengths.append([key,len(pd.read_hdf('data_2nd_scrub.h5',str(key)).index)])






st = time.time() 

def pnl_algo(key):  
    print(keys[key])
    min_data = []
    mid_data = 'mid_2nd_scrub.h5'
    all_data = 'data_2nd_scrub.h5'  
    t5 =0
    #old_min_value = -1
   
    current_data = pd.read_hdf(mid_data, str(keys[key])).reset_index(drop=True)
    current_data1 = pd.read_hdf(all_data, str(keys[key])).reset_index(drop=True)
    
    last_index = len(current_data.index)-1  
    
    min_in = min_inform(current_data.iloc[0:1000], current_data1.iloc[0:1000]) ##first min recorded and added to the the min data
    
    #ticks_out = [100, 250, 500, 750, 1000, 2000, 5000, 7000, 10000, 12000, 15000, 20000, 30000, 40000, 50000]
    ticks_out = [50,100]
    pnl = [pnl_calc(min_in[2], min_in[0], current_data, key, t) for t in ticks_out]
    
    new_row = np.array([min_in[0], min_in[2]])
    new_row = np.append(new_row, pnl)
    min_data.append(new_row)
   
# =============================================================================
#    
#     if keys[key] <= keys[-4]:
#         
#         if lengths[key+1][1] > 60000:
#             more_data = pd.read_hdf(all_data, str(keys[key+1])) 
#             all_data_frames = [current_data1, more_data]
#             more_mid = pd.read_hdf(mid_data, str(keys[key+1])) 
#             all_mid_frames = [current_data, more_mid]
#             
#         
#         elif lengths[key+1][1] + lengths[key+2][1] > 60000 :
#             
#             more_mid = [pd.read_hdf(mid_data, str(keys[key+1+k])) for k in range(2)]
#             all_mid_frames = [current_data, more_mid[0], more_mid[1]]
#             more_data = [pd.read_hdf(all_data, str(keys[key+1+k])) for k in range(2)]
#             all_data_frames = [current_data1, more_data[0], more_data[1]]
#         
#         
#         else:
#              more_mid = [pd.read_hdf(mid_data, str(keys[key+1+k])) for k in range(3)]
#              all_mid_frames = [current_data, more_mid[0], more_mid[1], more_mid[2]]
#              more_data = [pd.read_hdf(all_data, str(keys[key+1+k])) for k in range(3)]
#              all_data_frames = [current_data1, more_data[0], more_data[1], more_data[2]]
#         
# =============================================================================
        
        
        
    #elif keys[key] != keys[-1]:
    if keys[key] != keys[-1]:
        next_data = pd.read_hdf(all_data, str(keys[key+1]))
        all_data_frames = [current_data1, next_data]
        next_mid = pd.read_hdf(mid_data, str(keys[key+1]))
        all_mid_frames = [current_data, next_mid]
        
        
    else:
        next_data = pd.DataFrame([])
        all_data_frames = [current_data1, next_data]
        next_mid = pd.DataFrame([])
        all_mid_frames = [current_data, next_mid]
        
    s1 =time.time()  
    all_data = pd.concat(all_data_frames)
    mid_data = pd.concat(all_mid_frames)
    
    all_data = all_data.reset_index(drop=True)
    mid_data = mid_data.reset_index(drop=True)
    e1 =time.time()
    #print("t1:",e1-s1, mid_data['Mid'])
    
    #print(all_data)
    
    t = 0
    
    min_value = min_in[2]
    
    z1 = mid_data["Mid"]
    z2 = mid_data["Mid"]
    z3 = [mid_data["Mid"].iloc[t:] for t in ticks_out]
    
    
    s3 = time.time()
    #for i,x in enumerate(zip(z1,z2,z3[0],z3[1],z3[2],z3[3],z3[4],z3[5],z3[5],z3[6],z3[7],z3[8],z3[9],z3[10],z3[11],z3[12],z3[13],z3[14])):  
    for i,x in enumerate(zip(z1,z2,z3[0],z3[1])):
       
        if i > min_in[0]:
            if i <= last_index+999:
                
                if t < 999:
                    
                    if min_value > x[0]:
                        
                        min_value = x[0]  # min mid but we need to do calculations with the associated RateAsk, next line. 
                        min_value_used = x[1]
                       
                        pnl = [min_value_used-x[2+y] for y in range(len(ticks_out))]
                       
                        new_row = np.array([i, min_value])
                        new_row = np.append(new_row, pnl)
                        min_data.append(new_row)
                        
                        t = 0
                          
                    else:
                         t+=1 
                        
                else:
                    
                    min_value = 2
                    #print(i)
                    for it, y in enumerate(zip(mid_data["Mid"].iloc[i-999:i+1],mid_data["Mid"].iloc[i-999:i+1])):
                        #print(y[0],min_value)
                        if y[0] < min_value:
                             min_value = y[0]
                             min_value_used = y[1]
                             index = it+i-999 
                    
                    pnl = []
                    s5 =time.time()
                    d = [mid_data["Mid"].iloc[index:index+t] for t in ticks_out]
                    #for it_2, y in enumerate(zip(d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9],d[10],d[11],d[12],d[13],d[14])):
                    for it_2, y in enumerate(zip(d[0],d[1])):    
                        pnl = [min_value_used-x[y] for y in range(len(ticks_out))]
                        #if it_2 in ticks_out:
                            #pnl.append(min_value-y[0])
                            
                       
                    new_row = np.array([index, min_value])
                    new_row  = np.append(new_row, pnl)
                 
                    min_data.append(new_row)
                    t = 0 
                    e5 =time.time()
                    t5+=e5-s5
            else:
                pass
        else:
            pass
    
    e3 =time.time()
    #print("t3:",e3-s3)
    s4 = time.time()
    num = pd.DataFrame(min_data)
    num[0] = all_data.loc[num[0]]["RateDateTime"].reset_index(drop=True)
    e4 = time.time()
    #print("t4:",e4-s4)
    #print("t5:",t5)
    #old_pnl = pd.read_hdf('min_trans_pnl.h5',str(keys[key]))
    #for x in range(8):
    num[2] = -1*num[2]
    num[3] = -1*num[3]   
    #print(num)
    #num.to_hdf('min_trans_pnl.h5', str(key), format='table', append=True)        
    num.to_hdf('min_pnl_50-100.h5', str(key), format='table', append=True)


for x in range(len(keys)):
#for x in [761,762,763]:
    pnl_algo(x)            
       

et = time.time()
print(et-st)