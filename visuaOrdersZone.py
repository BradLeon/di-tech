# -*- coding: utf-8 -*-

'''
Author:  Lael
Created at: 2016-06-14 22:30  
该文件主要用来可视化 每天每个时间段出发以及到达的地点的情况
Finished at 2016/6/15 16:00 
'''



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
每个rush时间片的起点终点数目 startEndFragCountDic = {}
每个rush时间片终点不在66个区域之中的数目 startEndOutFragCountDic = {}


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
startEndFragCountDic = {start1:{frag1:{end1:count1,end2:count2,...}, frag2:{end1:count3, end2:count4,...},...}
startEndOutFragCountDic = {start1:{frag1:{endNo1:count1,endNo2:count2,...}, frag2:{endNo1:count3, endNo2:count4,...},...}
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
#需要预测的时间片                   
PREDICT_TIME_FIELDS= []
PREDICT_DATE = []


'''创建训练集中的日期'''
for i in range(1,22):
    if i < 10:
        date = '2016-01-0' + str(i)
    else:
        date = '2016-01-' + str(i)
    PREDICT_DATE.append(date)

'''将日期与星期几对应起来'''
DATE_TO_WEEK = {}
weekCount = 0
for date in PREDICT_DATE:
    if (5+weekCount)%7 == 0:
        DATE_TO_WEEK[date] = 7
    else:
        DATE_TO_WEEK[date] = (5 + weekCount)%7
    weekCount += 1


for i in range(1,22):
    if i < 10:
        date = '2016-01-0' + str(i)
    else:
        date = '2016-01-' + str(i)
    PREDICT_DATE.append(date)

'''生成日期与holiday的对应字典'''
IS_HOLIDAY_DIC = {}
HOLIDAY_LIST = ['2016-01-01','2016-01-02','2016-01-03','2016-01-09','2016-01-10','2016-01-16','2016-01-17','2016-01-23','2016-01-24','2016-01-30','2016-01-31']
#HOLIDAY_LIST = ['2016-01-01','2016-01-02','2016-01-03']
for date in PREDICT_DATE:
    if date in HOLIDAY_LIST:
        IS_HOLIDAY_DIC[date] = 1
    else:
        IS_HOLIDAY_DIC[date] = 0


class TrainOrderFeaturing():
    
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.orderPath = cwd + '\\season_2\\training_data\\order_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.predict_date = PREDICT_DATE
        self.time_field_request = {}
        self.time_field_request_destination = {}

        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.orderList = GFP.getFilePath(self.orderPath) 


        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for predictDate in self.predict_date:        
            for pos_field in range(1,67):
                pos_field = str(pos_field)
                self.time_field_request[pos_field] = {}
                self.time_field_request_destination[pos_field] = {}
                
    def readOrderData(self):
        '''
        从路径中一行一行的读取数据，将每一行的数据按照用户的id加入到相应的字典中
        例如：userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
        '''
        orderSet= set()
        count= 0
        lineCount = 0
        outZone = 0
        for order in self.orderList:
            #print 'order--:',order
            orderFile = file(order)
            for line in orderFile:
                order_id,driver_id,passenger_id,start_district_hash,dest_district_hash,Price,timePoint= line.split('\t')
                timePoint = timePoint.split('\n')[0]
                
                lineCount += 1
                #Date:2016-01-01
                Date = timePoint.split()[0]
                #Time:11:26:25
                Time = timePoint.split()[1]
                
                timeInfo = Time.split(':')
                hour = int(timeInfo[0])
                minute = int(timeInfo[1])
                time_field = hour*6 + minute/10 + 1
                timeFrag = Date + '-' + str(time_field)                
                
                #为每个区域的每个时间点添加一个订单价格信息
                pos_field = self.dist_hash2intDic[start_district_hash]
                pos_field = str(pos_field)
                
                try:
                    #统计区域内的请求情况
                    self.time_field_request[pos_field][timeFrag] += 1
                except:
                    self.time_field_request[pos_field][timeFrag] = 1


                #统计请求的终点情况
                try:
                    pos_field_destion = self.dist_hash2intDic[dest_district_hash]
                    pos_field_destion = str(pos_field_destion)
                    try:
                        #如果终点在66个区域之中则加进来
                        self.time_field_request_destination[pos_field_destion][timeFrag] += 1
                    except:
                        self.time_field_request_destination[pos_field_destion][timeFrag] = 1
                except:
                    #终点不在66个区域之中，则记录下来
                    outZone += 1
                
                
            orderFile.close()
        print 'lineCount:',lineCount  
        print 'length of self.time_field_request',len(self.time_field_request)
        print 'Out 66 zones: ',outZone
        return self.time_field_request,self.time_field_request_destination   

if __name__ == '__main__':
    print 'Reading data:',time.strftime('%Y-%m-%d %X',time.localtime())
    TOF = TrainOrderFeaturing()
    time_field_request,time_field_request_destination = TOF.readOrderData()
    #用来统计工作日早上6:30--8:00 [40,48]的区域与订单之间的关系  学校
    workOrdersFirstDic = {}
    workOrdersFirstDestDic = {}
    #用来统计工作日早上8:00--10:00 [49,60]的区域与订单之间的关系  公司
    workOrdersSecondDic = {}
    workOrdersSecondDestDic = {}
    #用来统计工作日下午17:00--18:00 [103,108] 的区域与订单之间的关系   学校
    workOrdersThirdDic = {}
    workOrdersThirdDestDic = {}
    #用来统计工作日下午17:00--20:00 [103,120] 的区域与订单之间的关系 公司
    workOrdersFourthDic = {}
    workOrdersFourthDestDic = {}
    #用来统计工作日晚上20:00--24:00 [121,144] 的区域与订单之间的关系  娱乐商圈等
    workOrdersNightDic = {}
    workOrdersNightDestDic = {}
    
    '''假日的情况用来和工作日的情况做对比，来区分不同地点的属性'''
    #用来节假日早上6:30--8:00 [40,48]的区域与订单之间的关系  学校
    holidayOrdersFirstDic = {}
    holidayOrdersFirstDestDic = {}
    #用来统计假日早上8:00--10:00 [49,60]的区域与订单之间的关系  公司
    holidayOrdersSecondDic = {}
    holidayOrdersSecondDestDic = {}
    #用来统计假日下午17:00--18:00 [103,108] 的区域与订单之间的关系   学校
    holidayOrdersThirdDic = {}
    holidayOrdersThirdDestDic = {}
    #用来统计假日下午17:00--20:00 [103,120] 的区域与订单之间的关系 公司
    holidayOrdersFourthDic = {}
    holidayOrdersFourthDestDic = {}
    #用来统计假日晚上20:00--24:00 [121,144] 的区域与订单之间的关系  娱乐商圈等
    holidayOrdersNightDic = {}
    holidayOrdersNightDestDic = {}
    #用来统计假日下午16:00--20:00 [97,120] 的区域与订单之间的关系   起点：景区，郊区，娱乐  终点：居民区，商圈
    holidayOrdersAfternoonDic = {}
    holidayOrdersAfternoonDestDic = {}
    
    #不在所取区间的订单数量
    count = 0
    count2 = 0
    #在所取时间片的订单数量
    countIn = 0
    for i in range(1,67):
        pos_field = str(i)
        #print len(time_field_request[pos_field]),str(i),'----------',
        for timeFrag,orderNum in time_field_request[pos_field].items():
            timeList = timeFrag.split('-')
            timeDate = timeList[0] + '-' + timeList[1] + '-' + timeList[2]
            frag = int(timeList[-1])
            #判断当天是否是节假日
            isHoliday = IS_HOLIDAY_DIC[timeDate]
            if isHoliday == 0:
                #如果时间片不在这些区域，那么默认不处理这些时间片
                if frag in range(40,49):
                    try:
                        workOrdersFirstDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersFirstDic[pos_field] = 1
                        countIn += 1
                if frag in range(49,61):
                    try:
                        workOrdersSecondDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersSecondDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,109):
                    try:
                        workOrdersThirdDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersThirdDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,120):
                    try:
                        workOrdersFourthDic[pos_field] += orderNum
                    except:
                        workOrdersFourthDic[pos_field] = 1
                if frag in range(121,144):
                    try:
                        workOrdersNightDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersNightDic[pos_field] = 1
                        countIn += 1
                if frag in range(1,40) or frag in range(61,103):
                    count += 1
                count2 += 1
            else:
                #如果时间片不在这些区域，那么默认不处理这些时间片
                if frag in range(40,49):
                    try:
                        holidayOrdersFirstDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersFirstDic[pos_field] = 1
                        countIn += 1
                if frag in range(49,61):
                    try:
                        holidayOrdersSecondDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersSecondDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,109):
                    try:
                        holidayOrdersThirdDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersThirdDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,120):
                    try:
                        holidayOrdersFourthDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersFourthDic[pos_field] = 1
                        countIn += 1
                if frag in range(121,144):
                    try:
                        holidayOrdersNightDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersNightDic[pos_field] = 1
                        countIn += 1
                if frag in range(97,121):
                    try:
                        holidayOrdersAfternoonDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersAfternoonDic[pos_field] = 1
                        countIn += 1
                if frag in range(1,40) or frag in range(61,97):
                    count += 1
                count2 += 1
                
        #计算终点的情况   
        for timeFrag,orderNum in time_field_request_destination[pos_field].items():
            timeList = timeFrag.split('-')
            timeDate = timeList[0] + '-' + timeList[1] + '-' + timeList[2]
            frag = int(timeList[-1])
            #判断当天是否是节假日
            isHoliday = IS_HOLIDAY_DIC[timeDate]
            if isHoliday == 0:
                #如果时间片不在这些区域，那么默认不处理这些时间片
                if frag in range(40,49):
                    try:
                        workOrdersFirstDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersFirstDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(49,61):
                    try:
                        workOrdersSecondDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersSecondDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,109):
                    try:
                        workOrdersThirdDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersThirdDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,120):
                    try:
                        workOrdersFourthDestDic[pos_field] += orderNum
                    except:
                        workOrdersFourthDestDic[pos_field] = 1
                if frag in range(121,144):
                    try:
                        workOrdersNightDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        workOrdersNightDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(1,40) or frag in range(61,103):
                    count += 1
                count2 += 1
            else:
                #如果时间片不在这些区域，那么默认不处理这些时间片
                if frag in range(40,49):
                    try:
                        holidayOrdersFirstDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersFirstDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(49,61):
                    try:
                        holidayOrdersSecondDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersSecondDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,109):
                    try:
                        holidayOrdersThirdDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersThirdDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(103,120):
                    try:
                        holidayOrdersFourthDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersFourthDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(121,144):
                    try:
                        holidayOrdersNightDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersNightDestDic[pos_field] = 1
                        countIn += 1
                if frag in range(97,121):
                    try:
                        holidayOrdersAfternoonDestDic[pos_field] += orderNum
                        countIn += 1
                    except:
                        holidayOrdersAfternoonDestDic[pos_field] = 1
                        countIn += 1
                
    workOrdersFirstList =  [workOrdersFirstDic[str(i)] for i in range(1,67)]  
    workOrdersSecondList = [workOrdersSecondDic[str(i)] for i in range(1,67)]
    workOrdersThirdList = [workOrdersThirdDic[str(i)] for i in range(1,67)] 
    workOrdersFourthList = [workOrdersFourthDic[str(i)] for i in range(1,67)]  
    workOrdersNightList = [workOrdersNightDic[str(i)] for i in range(1,67)] 
    holidayOrdersFirstList = [holidayOrdersFirstDic[str(i)] for i in range(1,67)]   
    holidayOrdersSecondList = [holidayOrdersSecondDic[str(i)] for i in range(1,67)] 
    holidayOrdersThirdList = [holidayOrdersThirdDic[str(i)] for i in range(1,67)] 
    holidayOrdersFourthList = [holidayOrdersFourthDic[str(i)] for i in range(1,67)]  
    holidayOrdersNightList = [holidayOrdersNightDic[str(i)] for i in range(1,67)]   
    holidayOrdersAfternoonList = [holidayOrdersAfternoonDic[str(i)] for i in range(1,67)]
    
    
    
    
    workOrdersFirstDestList =  [workOrdersFirstDestDic[str(i)] for i in range(1,67)]  
    workOrdersSecondDestList = [workOrdersSecondDestDic[str(i)] for i in range(1,67)]
    workOrdersThirdDestList = [workOrdersThirdDestDic[str(i)] for i in range(1,67)] 
    workOrdersFourthDestList = [workOrdersFourthDestDic[str(i)] for i in range(1,67)]  
    workOrdersNightDestList = [workOrdersNightDestDic[str(i)] for i in range(1,67)] 
    holidayOrdersFirstDestList = [holidayOrdersFirstDestDic[str(i)] for i in range(1,67)]   
    holidayOrdersSecondDestList = [holidayOrdersSecondDestDic[str(i)] for i in range(1,67)] 
    holidayOrdersThirdDestList = [holidayOrdersThirdDestDic[str(i)] for i in range(1,67)] 
    holidayOrdersFourthDestList = [holidayOrdersFourthDestDic[str(i)] for i in range(1,67)]  
    holidayOrdersNightDestList = [holidayOrdersNightDestDic[str(i)] for i in range(1,67)]   
    holidayOrdersAfternoonDestList = [holidayOrdersAfternoonDestDic[str(i)] for i in range(1,67)]    
    
    
    #融合起点和终点两种情况
    firstList = []
    secondList = []
    thirdList = []
    fourthList = []
    nightList = []
    holidayFirstList = []
    holidaySecondList = []
    holidayThirdList = []
    holidayFourthList = []
    holidayNightList = []
    holidayAfternoonList = []  
    for i in range(0,66):
        firstList.append([workOrdersFirstList[i],workOrdersFirstDestList[i]])
        secondList.append([workOrdersSecondList[i],workOrdersSecondDestList[i]])
        thirdList.append([workOrdersThirdList[i],workOrdersThirdDestList[i]])
        fourthList.append([workOrdersFourthList[i],workOrdersFourthDestList[i]])
        nightList.append([workOrdersNightList[i],workOrdersNightDestList[i]])
        holidayFirstList.append([holidayOrdersFirstList[i],holidayOrdersFirstDestList[i]])
        holidaySecondList.append([holidayOrdersSecondList[i],holidayOrdersSecondDestList[i]])
        holidayThirdList.append([holidayOrdersThirdList[i],holidayOrdersThirdDestList[i]])
        holidayFourthList.append([holidayOrdersFourthList[i],holidayOrdersFourthDestList[i]])
        holidayNightList.append([holidayOrdersNightList[i],holidayOrdersNightDestList[i]])
        holidayAfternoonList.append([holidayOrdersAfternoonList[i],holidayOrdersAfternoonDestList[i]])
    
    
                    
                  
    firstDF = pd.DataFrame(firstList,index=[str(i) for i in range(1,67)])
    firstDF.plot(kind='bar',rot = 1, title='workFirstDF----start--end--->weekday 6:30--8:00!',alpha=0.4)
    
    secondDF = pd.DataFrame(secondList,index=[str(i) for i in range(1,67)])
    secondDF.plot(kind='bar',rot = 1, title='workSecondList-----start--end---->weekday 8:00--10:00!',alpha=0.4)
    
    thirdDF = pd.DataFrame(thirdList,index=[str(i) for i in range(1,67)])
    thirdDF.plot(kind='bar',rot = 1, title='workOrdersThirdList-----start--end--->weekday 17:00--18:00!',alpha=0.4)
    
    fourthDF = pd.DataFrame(fourthList,index=[str(i) for i in range(1,67)])
    fourthDF.plot(kind='bar',rot = 1, title='workOrdersFourthList---start--end----->weekday 17:00--20:00!',alpha=0.4)
    
    nightDF = pd.DataFrame(nightList,index=[str(i) for i in range(1,67)])
    nightDF.plot(kind='bar',rot = 1, title='workOrdersNightList----start--end----->weekday 20:00--24:00!',alpha=0.4)
    
    
    print holidayFirstList,'---**&&'
    print holidayThirdList,'-------%%%%%'    
    
    holidayFirstDF = pd.DataFrame(holidayFirstList,index=[str(i) for i in range(1,67)])
    holidayFirstDF.plot(kind='bar',rot = 1, title='holidayOrdersFirstList----->holiday 6:30--8:00!!',alpha=0.4)
    
    holidaySecondDF = pd.DataFrame(holidaySecondList,index=[str(i) for i in range(1,67)])
    holidaySecondDF.plot(kind='bar',rot = 1, title='holidayOrdersSecondList----->holiday 8:00--10:00!!',alpha=0.4)
    
    holidayThirdDF = pd.DataFrame(holidayThirdList,index=[str(i) for i in range(1,67)])
    holidayThirdDF.plot(kind='bar',rot = 1, title='holidayOrdersThirdList------>holiday 17:00--18:00!!',alpha=0.4)
    
    holidayFourthDF = pd.DataFrame(holidayFourthList,index=[str(i) for i in range(1,67)])
    holidayFourthDF.plot(kind='bar',rot = 1, title='holidayOrdersFourthList-------->holiday 17:00--20:00!!',alpha=0.4)
    
    holidayNightDF = pd.DataFrame(holidayNightList,index=[str(i) for i in range(1,67)])
    holidayNightDF.plot(kind='bar',rot = 1, title='holidayOrdersNightList--------->holiday 20:00--24:00!!',alpha=0.4)
    
    holidayAfternoonDF = pd.DataFrame(holidayAfternoonList,index=[str(i) for i in range(1,67)])
    holidayAfternoonDF.plot(kind='bar',rot = 1, title='holidayOrdersAfternoonList--------->holiday 16:00--20:00!!',alpha=0.4)
    
    print '不在所取时间片中的订单数目为：',count
    print '在所取时间片中的订单数目为：',countIn
    print '总的订单数目为：',count2

    print 'Finish plotting:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
            
    

    
    
    
   