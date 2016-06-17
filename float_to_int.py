# -*- coding: utf-8 -*-
"""
Created on Sun Jun 05 22:57:52 2016

@author: Leon
"""


def function ():
    
    fp = open('D:\\di_tech\\dataset\\season_1\\result\\submission_gbrt_s2_0613_4.csv','r')
    writer = open('D:\\di_tech\\dataset\\season_1\\result\submission_gbrt_s2_0613_4_round.csv','w')
    for line in fp.readlines():
        pos, time , gap = line.strip().split(',')
        #print gap
        
        gap = float(gap)
        if gap < 1:
            gap = 1.0
        gap = float(round(gap))
            
        #print pos,time,gap
        
        writer.write( pos+','+time+','+str(gap)+'\n')
        
    writer.close()
        
        

function()