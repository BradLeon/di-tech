# -*- coding: utf-8 -*-


'''
该文件用来生成区域统计信息字典 distPriceListDic{}
用户订单信息字典userDic{}
区域请求字典time_field_request{}
区域响应字典time_field_response{}
拥堵字典trafficDic{}
poi字典poiDic = {}
顾客数usersDic = {}
司机数driversDic = {}
每天的订单数 ordersPerdayDic = {}

时间点 2016-01-22-122  以timeFrag标识

userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
distPriceListDic = {区域：{时间片:该时间片的有响应的订单的总价格)}...}
time_field_request{} = {区域1：{时间片1：该时间片的请求数目}，{时间片2：该时间片的请求数目}...}})
time_field_response{} = {区域2：{时间片1：该时间片的响应数目}，{时间片2：该时间片的响应数目}...}
trafficDic = {区域1：{时间片：[231,33,13,10]}，...}
poiDic = {区域1：[POI类目及数量信息],...}
usersDic = {区域2：{时间片1：顾客id_[]}}
driversDic = {区域1：{时间片1：司机id_[]}，...}
ordersPerdayDic = {day1:200000,day2:300000...}
'''



'''
2016/6/8  获取每一天的订单总数，每一天的GAP总数，并可视化
'''

from pandas import Series,DataFrame
import pandas as pd
import time
import os
from datetime import datetime
from cluster_map_parser import cluster_map
from GetPath import GetFilePath
from GetPOI2 import GetPoiDic
from GetTraffic2 import TrafficFeaturing
from GetWeather2 import WeatherFeaturing
#需要预测的时间片的前面半个小时时间片
DEMAND_TIME_FIELDS=[43,44,45,55,56,57,67,68,69,79,80,81,91,92,93,
                    103,104,105,115,116,117,127,128,129,139,140,141]



'''高峰时间LIST'''
RUSH_LIST = []
for i in range(45,60):
    RUSH_LIST.append(i)
for i in range(102,111):
    RUSH_LIST.append(i)
for i in range(124,134):
    RUSH_LIST.append(i)

class TrainOrderFeaturing():
    
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.orderPath = cwd + '\\season_2\\test_set_2\\order_data_1_2'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.time_field_request = {}
        self.time_field_response = {}
        self.usersDic = {}
        self.driversDic = {}
        self.userDic = {}
        self.distPriceListDic = {}
        self.gapDic = {}
        self.ordersPerdayDic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.orderList = GFP.getFilePath(self.orderPath) 

        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
      
        for pos_field in range(1,67): 
             pos_field = str(pos_field)
             
             self.distPriceListDic[pos_field] = {}
             self.time_field_request[pos_field] = {}
             self.time_field_response[pos_field] = {}
             self.usersDic[pos_field] = {}
             self.driversDic[pos_field] = {}
             self.gapDic[pos_field] = {}
                
    def readOrderData(self):
        '''
        从路径中一行一行的读取数据，将每一行的数据按照用户的id加入到相应的字典中
        例如：userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
        '''
        orderSet= set()
        count= 0
        for order in self.orderList:
            orderFile = file(order)
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
                
                if int(Date.split('-')[-1][0]) == 0:
                    day = int(Date.split('-')[-1][1])
                else:
                    day = int(Date.split('-')[-1][0])*10 + int(Date.split('-')[-1][1])
                
                '''统计每一天的订单数'''
                try:
                    self.ordersPerdayDic[day] += 1
                except Exception,ex:
                    self.ordersPerdayDic[day] = 1
                
                
                #如果该订单不是一个重复的订单
                #if order_id not in orderSet:
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
                    #统计区域内的请求情况
                    self.time_field_request[pos_field][timeFrag] += 1
                except:
                    self.time_field_request[pos_field][timeFrag] = 1
                
                #统计区域内的响应情况以及总的司机数目
                try:
                    self.time_field_response[pos_field][timeFrag] += 1
                except:
                    self.time_field_response[pos_field][timeFrag] = 1
                
                try:
                     self.driversDic[pos_field][timeFrag].append(driver_id)
                except:
                    self.driversDic[pos_field][timeFrag] = []
                    self.driversDic[pos_field][timeFrag].append(driver_id)
                try:
                    if len(self.distPriceListDic[pos_field][timeFrag]) >= 0:
                        self.distPriceListDic[pos_field][timeFrag].append(Price)
                except:
                    self.distPriceListDic[pos_field][timeFrag] = []
                    self.distPriceListDic[pos_field][timeFrag].append(Price)
                        
                if driver_id == 'NULL':
                    try:
                        self.gapDic[pos_field][timeFrag] += 1
                    except:
                        self.gapDic[pos_field][timeFrag] = 1
                    
                try:
                    '''统计所有的用户数目'''
                    self.usersDic[pos_field][timeFrag].append(passenger_id)
                except:
                    self.usersDic[pos_field][timeFrag] = []
                    self.usersDic[pos_field][timeFrag].append(passenger_id)
                 
                    #orderSet.add(order_id)
            orderFile.close()
               
        #print len(self.noRepeatUsersDic['2']['2016-01-22-92']),'____________+++++len(self.noRepeatUsersDic 2016-01-22-92'
        #print self.time_field_response,'-----------------------'                    
        return self.ordersPerdayDic,self.distPriceListDic,self.userDic,self.time_field_request,self.time_field_response,self.usersDic,self.driversDic,self.gapDic     

if __name__ == '__main__':
    print 'Reading data:',time.strftime('%Y-%m-%d %X',time.localtime())
    TOF = TrainOrderFeaturing()
    ordersPerdayDic,distPriceListDic,userDic,time_field_request,time_field_response,usersDic,driversDic,gapDic = TOF.readOrderData()
    WF = WeatherFeaturing()
    weatherDic,weatherTimeFields = WF.readWeatherData()
    
    '''得到每一个区域的...'''
   
    '''统计测试集中相邻两天的gap关系,首先统计出每一天的GAP数目'''
    gapTwoDayList = []
    gapPerDayDic = {}
    for i in range(1,67):
        for timeFrag,gap in gapDic[str(i)].items():
            fragList = timeFrag.split('-')
            if int(fragList[2][0]) == 0:
                date = int(fragList[2][1])
            else:
                date = int(fragList[2][0]) * 10 + int(fragList[2][1])
            try:
                gapPerDayDic[date] += gap
            except Exception,ex:
                gapPerDayDic[date] = gap
    
    for i in range(22,31):
        gapTwoDayList.append([gapPerDayDic[i],gapPerDayDic[i+1]])
    gapTwoDayDF = pd.DataFrame(gapTwoDayList,index=[i for i in range(22,31)])
    gapTwoDayDF.plot(kind='bar',rot=1,title='Comparing the gap between current and its next day(22-->31)!')
    print 'Finish plotting gapTwoDayDF of 10 days(22_23,23_24...): ',time.strftime('%Y-%m-%d %X',time.localtime())
    
    '''统计测试集中相邻两天每天三个高峰时间段的gap关系'''
    rushGapTwoDayList = []
    rushGapPerDayDic = {}
    for i in range(22,32):
        rushGapPerDayDic[i] = [0,0,0]
    for i in range(1,67):
        for timeFrag,gap in gapDic[str(i)].items():
            fragList = timeFrag.split('-')
            if int(fragList[2][0]) == 0:
                date = int(fragList[2][1])
            else:
                date = int(fragList[2][0]) * 10 + int(fragList[2][1])
            frag = int(fragList[3])
            if frag in range(45,60):
                rushGapPerDayDic[date][0] += gap
            elif frag in range(102,111):
                rushGapPerDayDic[date][1] += gap
            elif frag in range(124,134):
                rushGapPerDayDic[date][2] += gap
    for i in range(22,31):
        rushGapTwoDayList.append([rushGapPerDayDic[i][0],rushGapPerDayDic[i][1],rushGapPerDayDic[i][2],rushGapPerDayDic[i+1][0],rushGapPerDayDic[i+1][1],rushGapPerDayDic[i+1][2]])
    rushGapDF = pd.DataFrame(rushGapTwoDayList,index=[i for i in range(22,31)])
    rushGapDF.plot(kind='bar',rot=1,title='Gap of rush hours in 22--31!')
    print 'Finish plotting rushGapDF of 10 days(22_23,23_24...): ',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
  
    '''计算媒体天的平均天气状况'''
    weatherMeanDic = {}
    for timeFrag, weatherInfoList in weatherDic.items():
        fragList = timeFrag.split('-')
        if int(fragList[2][0]) == 0:
            date = int(fragList[2][1])
        else:
            date = int(fragList[2][0]) * 10 + int(fragList[2][1])
        try:
            weatherMeanDic[date][0] += weatherInfoList[0]
            weatherMeanDic[date][1] += weatherInfoList[1]
            weatherMeanDic[date][2] += weatherInfoList[2]/20
            weatherMeanDic[date][3] = weatherInfoList[3] + 1
        except:
            tempLi = [item for item in weatherInfoList]
            tempLi[2] = tempLi[2]/20
            tempLi.append(1)
            weatherMeanDic[date] = tempLi
    print '---->weatherDic',weatherMeanDic
    #得到1--21号的天气平均数据
    weatherMeanList = []
    for i in range(22,32):
        weatherNum = weatherMeanDic[i][3]
        weatherMeanList.append([j/weatherNum for j in weatherMeanDic[i]][:3])
    weatherMeanDF = pd.DataFrame(weatherMeanList,index=[str(i) for i in range(22,32)])
    weatherMeanDF.plot(kind='bar', rot=1, title='Mean weather info in 21 days!', alpha = 0.48)
    print '----->:',weatherMeanList
    print 'Finish plotting totalPrice:',time.strftime('%Y-%m-%d %X',time.localtime())
    
       

    print 'Finish at: ',time.strftime('%Y-%m-%d %X',time.localtime())
    