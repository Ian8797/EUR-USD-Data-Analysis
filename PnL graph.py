# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 13:42:12 2018

@author: ivaimberg
"""


def get_weeks(first_date, last_date):
    dateframes = []
    full_weeks = int(((last_date-first_date).total_seconds())/3600.0/24//7)+1#make sure this is a truncated int 
    week = np.timedelta64(7, "D")
    #print(full_weeks)
    first_date = np.datetime64(first_date, 'D')
    last_date = np.datetime64(last_date, 'D')
    
    for x in range(full_weeks):
        if x < full_weeks-1:
            dateframes.append([x*week+first_date, (x+1)*week+first_date])
        else:
            dateframes.append([x*week+first_date, last_date])
    return dateframes         



import h5py 
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import sys

#max_file = 'max_pnl.h5'                                            #Remake total Pnl graph 
max_file2 = 'max_pnl_ext.h5'
max_file = 'max_pnl_50-100.h5'
#min_file = 'min_pnl.h5'
min_file = 'min_pnl_50-100.h5'
with h5py.File(max_file, 'r') as f:
    thing = list(f.keys())   
    thing = [int(x) for x in thing]
 
keys = sorted(thing)

#print(keys)
min_to_next = pd.DataFrame([])
max_to_next = pd.DataFrame([])
"""
cumul_100 = 0
cumul_250 = 0
cumul_500 = 0
cumul_750 = 0
cumul_1000 = 0
cumul_2000 = 0
cumul_5000 = 0 
cumul_7000 = 0
cumul_10000 = 0
cumul_12000 = 0 
cumul_15000 = 0 
"""
# =============================================================================
# 
# #cols = ['Week Start','Week End','C_100','C_500','C_1000','C_250','C_750', 'C_2000', 'C_5000', 'C_7000','C_10000','C_12000','C_15000']
# #cols=['Week Start', 'Week End', 'C_100', 'C_250', 'C_500', 'C_750', 'C_1k', 'C_2K', 'C_5k', 'C_7k', 'C_10k', 'C_12k', 'C_15k', 'C_20k', 'C_30k', 'C_40k', 'C_50k','C_50']
# cols = ['Week Start', 'Week End','C_50', 'C_100']
# c_pnl = pd.DataFrame(columns= cols)
# time_list = []
# c_info = [0]*(len(cols)-2)
# col_list = [2,3]
# for key in keys: #keep in mind final week of whole process 
# 
#     min_data = pd.read_hdf(min_file,str(key))
#     max_data = pd.read_hdf(max_file, str(key))
#     
#     if key == 113:
#         max_data[0]=pd.read_hdf(max_file2,str(key))[0]
#         
#     
#     min_data = min_to_next.append(min_data)
#     max_data = max_to_next.append(max_data)
#     
#     
#     
#     max_t1 = (max_data[0].iloc[0])
#     max_t2 = (max_data[0].iloc[-1])
#     
#     c_full_info =[]
#     print(key,max_t1,max_t2)
#     t_frames = get_weeks(max_t1, max_t2)
#     
#     if t_frames[-1][0]+np.timedelta64(7,"D") == t_frames[-1][1]:
#         min_to_next = pd.DataFrame([])
#         max_to_next = pd.DataFrame([])
#     else:
#         min_to_next = min_data[min_data[0]>= t_frames[-1][0]]
#         max_to_next = max_data[max_data[0]>= t_frames[-1][0]]
#         t_frames = t_frames[:-1]
#         
#     #print(max_to_next)
#     
#     for t in range(len(t_frames)):
#         
#         c_info = [c_info[x]+max_data[np.logical_and(max_data[0]>= t_frames[t][0], max_data[0] < t_frames[t][1])][y].sum()+min_data[np.logical_and(min_data[0]>= t_frames[t][0], min_data[0] < t_frames[t][1])][y].sum() for x,y in enumerate(col_list)]
#         times = [t_frames[t][0],t_frames[t][1]]
#         
#         c_full_info.append(times+c_info)
#         #print(c_full_info)
# 
#         
# 
#   
#         #c_info.append([t_frames[t][0], t_frames[t][1], cumul_100, cumul_500, cumul_1000, cumul_250, cumul_750, cumul_2000, cumul_5000, cumul_7000, cumul_10000, cumul_12000, cumul_15000])
#        
#     
#       
#     c_pnl = c_pnl.append(pd.DataFrame(data=c_full_info, columns=cols))
# 
# 
# 
# c_pnl.to_hdf('all_pnl_50-100.h5',str(0))    
#     
# =============================================================================

c_pnl = pd.read_hdf('all_trans_cum_pnl2.h5',str(0))
c_pnl2 = pd.read_hdf('all_pnl_50-100.h5',str(0))
#print(c_pnl[85:])
    
import matplotlib as mpl
mpl.style.use('bmh')
    

ax =plt.axes()
plt.rcParams["figure.figsize"] = [8,4]
ax.set(xlabel='Year',ylabel='Dollars USD')

[plt.plot(c_pnl["Week Start"], c_pnl[t], label='Transaction Cost '+t[2:]+' ticks') for t in ['C_50','C_100']]
[plt.plot(c_pnl2["Week Start"], c_pnl2[t], label='Cumulative '+t[2:]+' ticks') for t in ['C_50','C_100']]
plt.legend(loc='center left', bbox_to_anchor=(1,0.5))





