# -*- coding: utf-8 -*-
from pandas import Series,DataFrame
import pandas as pd
import time
import os
from datetime import datetime
from cluster_map_parser import cluster_map
from GetPath import GetFilePath
from GetPOI import GetPoiDic

#需要预测的时间片的前面半个小时时间片
DEMAND_TIME_FIELDS=[43,44,45,55,56,57,67,68,69,79,80,81,91,92,93,
                    103,104,105,115,116,117,127,128,129,139,140,141]
#需要预测的时间片                   
PREDICT_TIME_FIELDS= [46,58,70,82,94,106,118,130,142]
PREDICT_DATE = ['2016-01-22','2016-01-24','2016-01-26','2016-01-28','2016-01-30']

class TestOrderFeaturing():
    
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.orderPath = cwd + '\\test_set_1\\order_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.predict_date = PREDICT_DATE
        self.time_field_request = {}
        self.time_field_response = {}
        self.noRepeatUsersDic = {}
        self.noRepeatDriversDic = {}
        self.userDic = {}
        self.tof_dist_Dic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.orderList = GFP.getFilePath(self.orderPath) 

        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for predictDate in self.predict_date:        
            for pos_field in range(1,67): 
                 pos_field = str(pos_field)
                 
                 self.tof_dist_Dic[pos_field] = {}
                 self.time_field_request[pos_field] = {}
                 self.time_field_response[pos_field] = {}
                 self.noRepeatUsersDic[pos_field] = {}
                 self.noRepeatDriversDic[pos_field] = {}
                 
                
    def readOrderData(self):
        '''
        从路径中一行一行的读取数据，将每一行的数据按照用户的id加入到相应的字典中
        例如：userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
        '''
        orderSet= set()
        count= 0
        count2 = 0
        for order in self.orderList:
            orderFile = open(order)
            for line in orderFile:
                order_id,driver_id,passenger_id,start_district_hash,dest_district_hash,Price,timePoint= line.split('\t')
                timePoint = timePoint.split('\n')[0]
                
                #Date:2016-01-01
                Date = timePoint.split()[0]
                #Time:11:26:25
                Time = timePoint.split()[1]
                
                timeInfo = Time.split(':')
                hour = int(timeInfo[0])
                minute = int(timeInfo[1])
                time_field = hour*6 + minute/10 + 1
                timeFrag = Date + '-' + str(time_field)    
                #print type(timeFrag),'++____'

                #如果该订单不是一个重复的订单
                if order_id not in orderSet:
                    #如果该用户还未读取进来
                    if passenger_id not in self.userDic:
                        self.userDic[passenger_id] = [[order_id,driver_id,start_district_hash,dest_district_hash,Price,Date,Time,timeFrag]]
                    else:
                        self.userDic[passenger_id].append([order_id,driver_id,start_district_hash,dest_district_hash,Price,Date,Time,timeFrag])
                    
                    #为每个区域的每个时间点添加一个订单价格信息
                    pos_field = self.dist_hash2intDic[start_district_hash]
                    pos_field = str(pos_field)
                    
                    '''判断各个值存在与否，如果不存在则新建'''

                        
                    try:
                        if self.time_field_request[pos_field][timeFrag] >= 0:
                            #统计区域内的请求情况
                            self.time_field_request[pos_field][timeFrag] += 1
                    except:
                        self.time_field_request[pos_field][timeFrag] = 1
                    
                  

                    if timeFrag == 2016-01-22-106:
                            print line,'-----'
                    #统计区域内的响应情况以及不重复的司机数目
                    if driver_id != 'NULL':
                        count2 += 1
                        
                        #print timeFrag,'+++++'

                        
                        try:
                            if self.time_field_response[pos_field][timeFrag] >= 0:
                                self.time_field_response[pos_field][timeFrag] += 1
                        except:
                            self.time_field_response[pos_field][timeFrag] = 1
                        
                        try:
                            if len(self.noRepeatDriversDic[pos_field][timeFrag]) >= 0:
                             self.noRepeatDriversDic[pos_field][timeFrag].add(driver_id)
                        except:
                            self.noRepeatDriversDic[pos_field][timeFrag] = set()
                            self.noRepeatDriversDic[pos_field][timeFrag].add(driver_id)
                        try:
                            if len(self.tof_dist_Dic[pos_field][timeFrag]) >= 0:
                                self.tof_dist_Dic[pos_field][timeFrag].add(Price)
                        except:
                            self.tof_dist_Dic[pos_field][timeFrag] = set()
                            self.tof_dist_Dic[pos_field][timeFrag].add(Price)
                            
                    try:
                        if len(self.noRepeatUsersDic[pos_field][timeFrag]) >=0:
                            '''统计不重复的用户数目'''
                            self.noRepeatUsersDic[pos_field][timeFrag].add(passenger_id)
                    except:
                        self.noRepeatUsersDic[pos_field][timeFrag] = set()
                        self.noRepeatUsersDic[pos_field][timeFrag].add(passenger_id)
                 
                    orderSet.add(order_id)
            orderFile.close()
        print count2
        #print len(self.noRepeatUsersDic['2']['2016-01-22-92']),'____________+++++len(self.noRepeatUsersDic 2016-01-22-92'
        #print self.time_field_response,'-----------------------'     
        print self.tof_dist_Dic['7']['2016-01-22-104'] 
        tp = 0.0
        for i in self.tof_dist_Dic['7']['2016-01-22-104']:
            p = float(i)
            tp += p
        print tp,len(self.tof_dist_Dic['7']['2016-01-22-104'])
        return self.tof_dist_Dic,self.userDic,self.time_field_request,self.time_field_response,self.noRepeatUsersDic,self.noRepeatDriversDic   
          
if __name__ == '__main__':
    TOF = TestOrderFeaturing()
    tdd,ud,tfr,tfr2,nru,nrd = TOF.readOrderData()
    

