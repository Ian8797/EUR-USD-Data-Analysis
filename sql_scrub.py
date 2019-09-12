# -*- coding: utf-8 -*-
"""
Created on Wed Jul 25 10:21:08 2018

@author: ivaimberg
"""
                                ###still need to scrub 2018 
import pymysql
import time

DB_NAME = 'EUR_USD_data'
table_names = ["data_"+str(2000+x) for x in range(19)]
#print(table_names)

#for x in [12,13,14,15,16,17]:                   
for x in [18]:
    print(table_names[x])
    
    row_counter = ("SELECT COUNT(*) FROM "+table_names[x]+" ")
    
    
   
    
    query_1 = ("CREATE TEMPORARY TABLE Ext_Mid_diff AS "                           
           "(WITH d1 AS (SELECT * From Mixed_year), d2 AS (SELECT * FROM Mixed_year) "
           "SELECT d1.LTid, d1.CurrencyPair, d1.RateDateTime, ((d2.RateBid+d2.RateAsk)/2 - (d1.RateBid+d1.RateAsk)/2) AS Mid_difference, d1.cDealable "
           "FROM d1 " 
           "INNER JOIN "+table_names[x]+" d2 ON d2.LTid = d1.LTid + 1000 "
           "ORDER BY ABS(Mid_difference) DESC "
           "LIMIT 2000); ")  
    
   


    extra = ("DESCRIBE Mid_diff;")
    
    
    ## returns table with differences between mid 1000 ticks apart from each other,
    query_2 = ("CREATE TEMPORARY TABLE Mid_diff AS "
               "(WITH d1 AS (SELECT * From Mixed_year), d2 AS (SELECT * FROM Mixed_year) "
               "SELECT d1.LTid, d1.CurrencyPair, d1.RateDateTime, ((d2.RateBid+d2.RateAsk)/2 - (d1.RateBid+d1.RateAsk)/2) AS Mid_difference, d1.cDealable "
               "FROM d1 " 
               "INNER JOIN "+table_names[x]+" d2 ON d2.LTid = d1.LTid + 1000); ")
               
    #this catches bad ticks 
    query_3 = ("CREATE TEMPORARY TABLE drop_table AS "
               "(SELECT Mid_diff.LTid FROM Mid_diff "
               "INNER JOIN Ext_Mid_diff ON Mid_diff.LTid = Ext_Mid_diff.LTid + 1000 "
               "WHERE Mid_diff.Mid_difference/Ext_Mid_diff.Mid_difference < -.8); ") 
             
              
    scrub_query = ("DELETE "+table_names[x]+" FROM "+table_names[x]+" "
                   "LEFT JOIN drop_table ON "+table_names[x]+".LTid = drop_table.LTid  "
                   "WHERE drop_table.LTid IS NOT NULL; ") 
                   #"WHERE data_2000_1.LTid > 304690 " 
                   #"LIMIT 1000; ")
                   
    """ #redoing Mixed_year 
    if x < 18:
        table_mix = ("CREATE TEMPORARY TABLE Mixed_year AS "
                     "(SELECT * FROM "+table_names[x]+" " 
                     "UNION SELECT * FROM "+table_names[x+1]+" WHERE "+table_names[x+1]+".LTid < 1001) ; ")
    else:
        table_mix = ("CREATE TEMPORARY TABLE Mixed_year AS "
                     "(SELECT * FROM "+table_names[x]+" ); ")
                     #"UNION SELECT * FROM "+table_names[x+1]+" WHERE "+table_names[x+1]+".LTid < 1001) ; ")
    """
    
    
    
             
    # scrubbing solved for each file individually
    ## to solve for all file start with the year you want plus first X rows of next year then all the operations should stay the same 
    
    chunk_size = 100000 ##when scrubbing add 1000 ticks to make sruc actually complete       
    cnn = pymysql.connect(host='localhost', user='root', password='Ivaim1997%', db='EUR_USD_data' , charset='utf8mb4')
    cursor = cnn.cursor()
    
    cursor.execute(row_counter)
    rows = cursor.fetchall()
    loops_needed = int((rows[0][0]//chunk_size + 1))
    last_rows = rows[0][0]%chunk_size 
    print(loops_needed, last_rows)
    
    cursor.close()
    cnn.close()
    
    for i in range(loops_needed):
        
        cnn = pymysql.connect(host='localhost', user='root', password='Ivaim1997%', db='EUR_USD_data' , charset='utf8mb4')
        cursor = cnn.cursor()
        """
        if i == loops_needed-1:
            
            table_mix = ("CREATE TEMPORARY TABLE Mixed_year AS "
                         "(SELECT * FROM "+table_names[x]+" "
                         "WHERE LTid > "+str(i*100000)+" "
                         "UNION SELECT * FROM "+table_names[x+1]+" WHERE "+table_names[x+1]+".LTid <= 1000); ")
            print(table_mix)
            
        else:
            table_mix = ("CREATE TEMPORARY TABLE Mixed_year AS  
                         "(SELECT * FROM "+table_names[x]+" "
                         "LIMIT "+str(0+i*100000)+", 101000); ")  ## use this for 2018 scrub 
            print(table_mix)
         """
        table_mix = ("CREATE TEMPORARY TABLE Mixed_year AS " 
                     "(SELECT * FROM "+table_names[x]+" "
                     "LIMIT "+str(0+i*100000)+", 101000); ")  ## use this for 2018 scrub 
        print(table_mix)
         
        
    
        
        start = time.time()
        
        cursor.execute(table_mix) #adds 1000 rows of next year data to selected year 
        lap = time.time()
        print(lap-start)
        
        
        cursor.execute(query_1) # creates table of 1000?? most extreme mid tick to 1000 tick differences 
        lap2 = time.time()
        print(lap2-lap)
        
        cursor.execute(query_2) # creates table of all mid tick to 1000 tick differences
        lap3 = time.time()
        print(lap3-lap2)
        
        cursor.execute(query_3) # creates table of LTid's where ticks hit the drop threshold
        lap4 = time.time()
        print(lap4-lap3)
        #cursor.execute("select * from drop_table; ")
        #a = cursor.fetchall()
        # print(a)
        cursor.execute(scrub_query) # drops these LTid rows from current year to finish the scrub 
        cursor.execute("DROP TABLE IF EXISTS Mixed_year;")
        cursor.execute("DROP TABLE IF EXISTS Ext_Mid_diff;")
        cursor.execute("DROP TABLE IF EXISTS Mid_diff;")
        cursor.execute("DROP TABLE IF EXISTS drop_table;")
        lap5 = time.time()
        print(lap5-lap4)
        print("100 thousand scrub:"+str(lap5-start))
        
    
     
    #end = time.time()
    #print(table_names[x]+" complete drop time: "+str(end-start))
    
    
        cnn.commit() 
        cursor.close()
        cnn.close()
