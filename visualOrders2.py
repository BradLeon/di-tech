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



'''
2016/6/8  获取每一天的订单总数，每一天的GAP总数，并可视化
2016/6/13 添加了每个时间片的订单计数，用来可视化每个rush区间每个订单起点所占比例，终点所占比例以及（起点，终点）所占比例
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


'''去除掉元旦三天的数据'''
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


class TrainOrderFeaturing():
    
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.orderPath = cwd + '\\season_2\\training_data\\order_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.predict_date = PREDICT_DATE
        self.time_field_request = {}
        self.time_field_response = {}
        self.usersDic = {}
        self.driversDic = {}
        self.userDic = {}
        self.distPriceListDic = {}
        self.gapDic = {}
        self.ordersPerdayDic = {}
        self.startEndFragCountDic = {}
        self.startEndOutFragCountDic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.orderList = GFP.getFilePath(self.orderPath) 

        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for predictDate in self.predict_date:        
            for pos_field in range(1,67):
                pos_field = str(pos_field)
                self.distPriceListDic[pos_field] = {}
                self.time_field_request[pos_field] = {}
                self.time_field_response[pos_field] = {}
                self.usersDic[pos_field] = {}
                self.driversDic[pos_field] = {}
                self.gapDic[pos_field] = {}
                self.startEndFragCountDic[pos_field] = {}
                self.startEndOutFragCountDic[pos_field] = {}
                #为7个 rush 区间 的订单起点终点计数信息赋初值
                for i in range(7):
                    self.startEndFragCountDic[pos_field][i] = {}
                    self.startEndOutFragCountDic[pos_field][i] = {}
                
    def readOrderData(self):
        '''
        从路径中一行一行的读取数据，将每一行的数据按照用户的id加入到相应的字典中
        例如：userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
        '''
        orderSet= set()
        count= 0
        lineCount = 0
        for order in self.orderList:
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
                
                '''判断各个值存在与否，如果不存在则新建'''
                rush_time_field = 0
                if time_field in range(45,50):
                    rush_time_field = 0
                elif time_field in range(50,55):
                    rush_time_field = 1
                elif time_field in range(55,60):
                    rush_time_field = 2
                elif time_field in range(102,107):
                    rush_time_field = 3
                elif time_field in range(107,112):
                    rush_time_field = 4
                elif time_field in range(124,129):
                    rush_time_field = 5
                elif time_field in range(129,134):
                    rush_time_field = 6
                else:
                    #如果时间片不在这几个rush区间，这里就不统计了
                    continue
                #有可能订单的终点不在66个区域之中
                try:
                    dest_field = self.dist_hash2intDic[dest_district_hash]
                except:
                    try:
                        self.startEndOutFragCountDic[pos_field][rush_time_field][dest_district_hash] += 1
                    except:
                        self.startEndOutFragCountDic[pos_field][rush_time_field][dest_district_hash] = 1
                    count += 1
                    #如果终点不在66个区域之中，那么就不再计算下面的终点在66个区域之中的情况了
                    continue
                
                dest_field = str(dest_field) 
                try:
                    self.startEndFragCountDic[pos_field][rush_time_field][dest_field] += 1
                except:
                    self.startEndFragCountDic[pos_field][rush_time_field][dest_field] = 1
                 

            orderFile.close()
        print 'count: ',count
        print 'lineCount:',lineCount
        #print len(self.noRepeatUsersDic['2']['2016-01-22-92']),'____________+++++len(self.noRepeatUsersDic 2016-01-22-92'
        #print self.time_field_response,'-----------------------'                    
        return self.ordersPerdayDic,self.distPriceListDic,self.userDic,self.time_field_request,self.time_field_response,self.usersDic,self.driversDic,self.gapDic,self.startEndFragCountDic,self.startEndOutFragCountDic     

if __name__ == '__main__':
    print 'Reading data:',time.strftime('%Y-%m-%d %X',time.localtime())
    TOF = TrainOrderFeaturing()
    ordersPerdayDic,distPriceListDic,userDic,time_field_request,time_field_response,usersDic,driversDic,gapDic,startEndFragCountDic,startEndOutFragCountDic = TOF.readOrderData()
    TRAFFIC = TrafficFeaturing()
    WEATHER = WeatherFeaturing()
    weatherDic,weatherTimeFields = WEATHER.readWeatherData()
    trafficDic = TRAFFIC.readTrafficData()
    
    '''得到每一个区域的...'''
    
    '''计算66个区域在7个区间21天的各自的总gap'''   
    gapRushDic = {}
    for i in range(1,67):
        gapRushDic[str(i)] = {0:0,1:0,2:0,3:0,4:0,5:0,6:0} 
        for timeFrag,gap in gapDic[str(i)].items():
            frag = int(timeFrag.split('-')[-1])
            if frag in range(45,50):
                frag = 0
            elif frag in range(50,55):
                frag = 1
            elif frag in range(55,60):
                frag = 2
            elif frag in range(102,107):
                frag = 3
            elif frag in range(107,112):
                frag = 4
            elif frag in range(124,129):
                frag = 5
            elif frag in range(129,134):
                frag = 6
            else:
                continue
            try:
                gapRushDic[str(i)][frag] += 1
            except:
                gapRushDic[str(i)][frag] = 1
    gapRushList = []
    for i in range(1,67):
        gapRushList.append(gapRushDic[str(i)].values())
    gapRushDF = pd.DataFrame(gapRushList,index=[str(i) for i in range(1,67)])
    gapRushDF.plot(kind='bar',rot=1,title='Gaps of seven timefrag in each 66 zones of 21 days!!!',alpha=0.4,stacked=True)
    print 'Finish plotting gaps in rush hours:',time.strftime('%Y-%m-%d %X',time.localtime())
               
    
    '''  
        每个区域每个rush时间段的起点和终点都在66个区域中的订单数量的处理
    '''
    '''sevenFragsOrdersNumDic={start1:{frag1:num1,frag2:num2,...frag7:num7},...start66:{}}'''
    sevenFragsOrdersNumDic = {}
    sevenFragsOrdersList = []
    for i in range(1,67):
        sevenFragsOrdersNumDic[str(i)] = {}
        for j in range(7):
            try:
                sevenFragsOrdersNumDic[str(i)][j] += sum(startEndFragCountDic[str(i)][j].values()) 
            except:
                sevenFragsOrdersNumDic[str(i)][j] = sum(startEndFragCountDic[str(i)][j].values())
            
        sevenFragsOrdersList.append([sevenFragsOrdersNumDic[str(i)][j] for j in range(7)])
    sevenFragsOrdersDF = pd.DataFrame(sevenFragsOrdersList,index=[str(i) for i in range(1,67)])
    sevenFragsOrdersDF.plot(kind='bar',rot=1,title='Orders of seven rush frags in each district of 21 days!!!',alpha=0.45,stacked=True)
    print 'Finish plotting Orders of seven rush frags in each district of 21 days:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
    
    '''  
        每个区域每个rush时间段的起点在但终点不在66个区域中的订单数量的处理,以加out来区分
    '''
    '''sevenFragsOrdersOutNumDic={start1:{frag1:num1,frag2:num2,...frag7:num7},...start66:{}}'''
    sevenFragsOrdersOutNumDic = {}
    sevenFragsOrdersOutList = []
    for i in range(1,67):
        sevenFragsOrdersOutNumDic[str(i)] = {}
        for j in range(7):
            try:
                sevenFragsOrdersOutNumDic[str(i)][j] += sum(startEndOutFragCountDic[str(i)][j].values()) 
            except:
                sevenFragsOrdersOutNumDic[str(i)][j] = sum(startEndOutFragCountDic[str(i)][j].values())
            
        sevenFragsOrdersOutList.append([sevenFragsOrdersOutNumDic[str(i)][j] for j in range(7)])
    sevenFragsOrdersOutDF = pd.DataFrame(sevenFragsOrdersOutList,index=[str(i) for i in range(1,67)])
    sevenFragsOrdersOutDF.plot(kind='bar',rot=1,title='Orders of seven rush frags in each district of 21 days(dest out of the zones)!!!',alpha=0.45,stacked=True)
    print 'Finish plotting rders of seven rush frags in each district of 21 days(dest out of the zones):',time.strftime('%Y-%m-%d %X',time.localtime())
        
        
    
    '''计算每个区域的总的price'''
    totalPriceDic = {}
    for i in range(1,67):
        pos_field = str(i)
        totalPrice = 0
        totalGap = 0
        for timeFrag,priceList in distPriceListDic[pos_field].items():
            priceList = map(float,priceList)
            totalPrice += sum(priceList)
        for timeFrag,gap in gapDic[pos_field].items():
            '''这里必须是一个列表，才能够绘图成功'''
            totalPriceDic[pos_field] = [totalPrice]
    totalPriceList = []
    for i in range(1,67):
        totalPriceList.append(totalPriceDic[str(i)])        
    totalPriceDF = pd.DataFrame(totalPriceList,index=[str(i) for i in range(1,67)])
    totalPriceDF.plot(kind='bar', rot = 1, title='Total price of 21 days in each Zone!',alpha=0.5)
    print 'Finish plotting totalPrice:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
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
    #print '---->weatherDic',weatherMeanDic
    #得到1--21号的天气平均数据
    weatherMeanList = []
    for i in range(1,22):
        weatherNum = weatherMeanDic[i][3]
        weatherMeanList.append([j/weatherNum for j in weatherMeanDic[i]][:3])
    weatherMeanDF = pd.DataFrame(weatherMeanList,index=[str(i) for i in range(1,22)])
    weatherMeanDF.plot(kind='bar', rot=1, title='Mean weather info in 21 days!', alpha = 0.48)
    #print '----->:',weatherMeanList
    print 'Finish plotting totalPrice:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
    '''统计第七天和前一周的同天及前一天之间的订单数对比情况'''
    ordersPerdayList = []
    for i in range(8,22):
        ordersPerdayList.append([ordersPerdayDic[i],ordersPerdayDic[i-7]])
    ordersPerdayDF = pd.DataFrame(ordersPerdayList,index=[str(i) for i in range(8,22)])
    ordersPerdayDF.plot(kind='bar', rot=1, title='Comparing the orders between present and its last 7th day(81,92...)!',alpha=0.4)
    print 'Finish plotting ordersPerday:',time.strftime('%Y-%m-%d %X',time.localtime())
        
        
        
    
    '''计算每个区域对应的总的gap'''    
    gapList = []
    for i in range(1,67):
        pos_field = str(i)
        #gapInField用来标记在每个区域中的gap值
        gapInField = 0
        for timeFrag,gap in gapDic[pos_field].items():
            gapInField += gap
        gapList.append(gapInField)
    totalGapDF = pd.DataFrame(gapList,index=[str(i) for i in range(1,67)])
    totalGapDF.plot(kind='bar',rot = 1, title='Total gap of 21 days in each Zone!',alpha=0.4)
    print 'Finish plotting totalGap:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
            
    '''统计哪些时间片的gap数目最多'''
    gapTimefragDic = {}
    for i in range(1,67):
        for timeFrag,gap in gapDic[str(i)].items():
            frag = timeFrag.split('-')[-1]
            try:
                gapTimefragDic[frag] += gap
            except:
                gapTimefragDic[frag] = gap
    #将字典转换成list
    gapTimefragList = []
    for i in range(1,145):
        try:
            gapTimefragList.append(gapTimefragDic[str(i)])
        except Exception,ex:
            print Exception,ex
    gapTimefragDF = pd.DataFrame(gapTimefragList,index=[str(i) for i in range(1,145)])
    gapTimefragDF.plot(kind='bar',rot=1,title='Total gap of 21 days in each time fragment!',alpha=0.6)
    print 'Finish plotting totalGap of each time fragment:',time.strftime('%Y-%m-%d %X',time.localtime())
    

    
    '''统计第七天和前一周的同天及前一天之间的gap关系,首先统计出每一天的GAP数目'''
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
    for i in range(8,22):
        gapTwoDayList.append([gapPerDayDic[i],gapPerDayDic[i-7]])
    gapTwoDayDF = pd.DataFrame(gapTwoDayList,index=[i for i in range(8,22)])
    gapTwoDayDF.plot(kind='bar',rot=1,title='Comparing the gap between its last 7th day!')
    print 'Finish plotting gapTwoDayDF of 14 days(81,92...): ',time.strftime('%Y-%m-%d %X',time.localtime())

    '''计算每一天的gap总数为'''
    gapPerDayList = []
    for date in range(1,22):
        gapPerDayList.append(gapPerDayDic[date])
    gapPerDayDF  = pd.DataFrame(gapPerDayList,index=[i for i in range(1,22)])
    gapPerDayDF.plot(kind='bar',rot=1,title='Gaps of each day(1--21)!!')
    print 'Finish plotting gapPerDayDF of 21 days(1,2...21): ',time.strftime('%Y-%m-%d %X',time.localtime())
    
    
    
    '''统计21天的交通数据,每一天的总的交通状况以及每一天RUSH HOUR的交通状况'''
    dateTrafficDic = {}
    dateTrafficRushDic = {}
    for i in range(1,22):
        dateTrafficDic[i] = [0,0,0,0]
        dateTrafficRushDic[i] = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]                
    for i in range(1,67):
        for timeFrag,trafficList in trafficDic[str(i)].items():
            fragList = timeFrag.split('-')
            if int(fragList[2][0]) == 0:
                date = int(fragList[2][1])
            else:
                date = int(fragList[2][0]) * 10 + int(fragList[2][1])
            frag = int(fragList[3])
            dateTrafficDic[date][0] += trafficList[0]
            dateTrafficDic[date][1] += trafficList[1]
            dateTrafficDic[date][2] += trafficList[2]
            dateTrafficDic[date][3] += trafficList[3]
                     
            if frag in range(45,50):
                dateTrafficRushDic[date][0][0] += trafficList[0]
                dateTrafficRushDic[date][0][1] += trafficList[1]
                dateTrafficRushDic[date][0][2] += trafficList[2]
                dateTrafficRushDic[date][0][3] += trafficList[3]
            elif frag in range(50,55):
                dateTrafficRushDic[date][1][0] += trafficList[0]
                dateTrafficRushDic[date][1][1] += trafficList[1]
                dateTrafficRushDic[date][1][2] += trafficList[2]
                dateTrafficRushDic[date][1][3] += trafficList[3]
            elif frag in range(55,60):
                dateTrafficRushDic[date][2][0] += trafficList[0]
                dateTrafficRushDic[date][2][1] += trafficList[1]
                dateTrafficRushDic[date][2][2] += trafficList[2]
                dateTrafficRushDic[date][2][3] += trafficList[3]
            elif frag in range(102,107):
                dateTrafficRushDic[date][3][0] += trafficList[0]
                dateTrafficRushDic[date][3][1] += trafficList[1]
                dateTrafficRushDic[date][3][2] += trafficList[2]
                dateTrafficRushDic[date][3][3] += trafficList[3]
            elif frag in range(107,112):
                dateTrafficRushDic[date][4][0] += trafficList[0]
                dateTrafficRushDic[date][4][1] += trafficList[1]
                dateTrafficRushDic[date][4][2] += trafficList[2]
                dateTrafficRushDic[date][4][3] += trafficList[3]
            elif frag in range(124,129):
                dateTrafficRushDic[date][5][0] += trafficList[0]
                dateTrafficRushDic[date][5][1] += trafficList[1]
                dateTrafficRushDic[date][5][2] += trafficList[2]
                dateTrafficRushDic[date][5][3] += trafficList[3]
            elif frag in range(129,134):
                dateTrafficRushDic[date][6][0] += trafficList[0]
                dateTrafficRushDic[date][6][1] += trafficList[1]
                dateTrafficRushDic[date][6][2] += trafficList[2]
                dateTrafficRushDic[date][6][3] += trafficList[3]
           
    dateTrafficList = []
    dateTrafficRushList = []
    for i in range(1,22):
        dateTrafficList.append(dateTrafficDic[i])
        dateTrafficRushList.append(dateTrafficRushDic[i][0])
        dateTrafficRushList.append(dateTrafficRushDic[i][1])
        dateTrafficRushList.append(dateTrafficRushDic[i][2])
        dateTrafficRushList.append(dateTrafficRushDic[i][3])
        dateTrafficRushList.append(dateTrafficRushDic[i][4])
        dateTrafficRushList.append(dateTrafficRushDic[i][5])
        dateTrafficRushList.append(dateTrafficRushDic[i][6])
  
    dateTrafficDF = pd.DataFrame(dateTrafficList,index=[i for i in range(1,22)])  
    dateTrafficRushDF = pd.DataFrame(dateTrafficRushList,index=[i for i in range(1,148)])
    dateTrafficDF.plot(kind='bar',rot=1,title='Traffic info of each day!!', stacked=True)
    dateTrafficRushDF.plot(kind='bar',rot=1,title='Rush hour traffic info of each day!!',stacked=True)
    print 'Finish plot traffic information at: ',time.strftime('%Y-%m-%d %X',time.localtime())
    