# -*- coding: utf-8 -*-

'''
该文件用来生成区域统计信息字典 tof_dist_Dic{}
用户订单信息字典userDic{}
区域请求字典request{}
区域响应字典response{}
拥堵字典trafficDic{}
poi字典poiDic = {}
顾客数usersDic = {}
司机数driversDic = {}

时间点 2016-01-22-122  以timeFrag标识

userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
totalPriceDic = {区域：{时间片:该时间片的订单的总价格)}...}
requestDic{} = {区域1：{时间片1：该时间片的请求数目}，{时间片2：该时间片的请求数目}...}})
responseDic{} = {区域2：{时间片1：该时间片的响应数目}，{时间片2：该时间片的响应数目}...}
trafficDic = {区域1：{时间片：[231,33,13,10]}，...}
poiDic = {区域1：[POI类目及数量信息],...}
usersDic = {区域2：{时间片1：顾客id_[]}}
driversDic = {区域1：{时间片1：司机id_[]}，...}
transOrders = {区域1：{时间片1：该时间片订单始末点不在一个区域的订单数量}}
'''



'''
2016/6/9 21:08 重新在TrainOrderFeaturing0609的基础上新生成的测试集

1. 当前的时间片ID
2. 是否是节假日
3. 目的地和出发点不是同一个的订单数
4. 是否是高峰期 7:30--10:00 18:30--20:00  46--60 105--120
'''

from pandas import Series,DataFrame
import pandas as pd
import time
import os
from datetime import datetime
from cluster_map_parser import cluster_map
from GetPath import GetFilePath
from GetPOI2 import GetPoiDic

#需要预测的时间片的前面半个小时时间片
DEMAND_TIME_FIELDS=[43,44,45,55,56,57,67,68,69,79,80,81,91,92,93,
                    103,104,105,115,116,117,127,128,129,139,140,141]
#需要预测的时间片                   
PREDICT_TIME_FIELDS= [46,58,70,82,94,106,118,130,142]
PREDICT_DATE = []


PREDICT_DATE = ['2016-01-23','2016-01-25','2016-01-27','2016-01-29','2016-01-31']

'''将日期与星期几对应起来'''
DATE_TO_WEEK = {'2016-01-23' : 6, '2016-01-25':1,'2016-01-27':3,'2016-01-29':5,'2016-01-31':7}


'''生成日期与holiday的对应字典'''
IS_HOLIDAY_DIC = {}
HOLIDAY_LIST = ['2016-01-01','2016-01-02','2016-01-03','2016-01-09','2016-01-10','2016-01-16','2016-01-17','2016-01-23','2016-01-24','2016-01-30','2016-01-31']
for date in PREDICT_DATE:
    if date in HOLIDAY_LIST:
        IS_HOLIDAY_DIC[date] = 1
    else:
        IS_HOLIDAY_DIC[date] = 0
        
'''高峰时间LIST'''
RUSH_LIST = []
for i in range(46,61):
    RUSH_LIST.append(i)
for i in range(105,121):
    RUSH_LIST.append(i)

class TestOrderFeaturing():
    
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.orderPath = cwd + '\\season_2\\test_set_2\\order_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.predict_date = PREDICT_DATE
        self.requestDic = {}
        self.responseDic = {}
        self.transOrders = {}
        self.totalPriceDic = {}

        self.tof_dist_Dic = {}
        self.gapDic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.orderList = GFP.getFilePath(self.orderPath) 

        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for predictDate in self.predict_date:        
            for pos_field in range(1,67): 
                 pos_field = str(pos_field)
                 self.transOrders[pos_field] = {}
                 self.totalPriceDic[pos_field] = {}
                 self.requestDic[pos_field] = {}
                 self.responseDic[pos_field] = {}
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
                order_id,driver_id,passenger_id,start_district_hash,dest_district_hash,price,timePoint= line.split('\t')
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
               
                #为每个区域的每个时间点添加一个订单价格信息
                pos_field = self.dist_hash2intDic[start_district_hash]
                pos_field = str(pos_field)
                
                '''判断各个值存在与否，如果不存在则新建'''                    
                try:
                    #统计区域内的请求情况
                    self.requestDic[pos_field][timeFrag] += 1
                except:
                    self.requestDic[pos_field][timeFrag] = 1
                
                if start_district_hash != dest_district_hash:
                    try:
                        self.transOrders[pos_field][timeFrag] += 1
                    except:
                        self.transOrders[pos_field][timeFrag] = 1
                        
                if driver_id != 'NULL':                                   
                    #统计区域内的响应情况
                    try:
                        self.responseDic[pos_field][timeFrag] += 1
                    except:
                        self.responseDic[pos_field][timeFrag] = 1
                        
                try:       
                    self.gapDic[pos_field][timeFrag] = self.requestDic[pos_field][timeFrag] - len(self.responseDic[pos_field][timeFrag])
                except:
                    '''如果driversDic中没有该时间片，说明该时间片中没有司机应答，那么用户的请求数量就能表示该区域该时间片的gap值'''
                    self.gapDic[pos_field][timeFrag] = self.requestDic[pos_field][timeFrag]                
                        
                try:
                    self.totalPriceDic[pos_field][timeFrag] += float(price)
                except:
                    self.totalPriceDic[pos_field][timeFrag] = float(price)
                        
                try:
                    self.gapDic[pos_field][timeFrag] = self.requestDic[pos_field][timeFrag] - self.responseDic[pos_field][timeFrag]
                except Exception,ex:
                    self.gapDic[pos_field][timeFrag] = 1
              #  print 'self.totalPriceDic[pos_field][timeFrag]---->',self.totalPriceDic[pos_field][timeFrag]

            orderFile.close()
            
        return self.totalPriceDic,self.requestDic,self.responseDic,self.gapDic,self.transOrders          
        
#获取拥堵信息
class TrafficFeaturing():
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.trafficPath = cwd + '\\season_2\\test_set_2\\traffic_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.predict_date = PREDICT_DATE
        self.trafficDic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.trafficList = GFP.getFilePath(self.trafficPath)        
        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for predictDate in self.predict_date:
            for pos_field in range(1,67):
                pos_field = str(pos_field)
                self.trafficDic[pos_field] = {}
                
                '''
                for time_field in self.demand_time_fields:
                    predictFrag = predictDate + '-' + str(time_field)
                    self.trafficDic[pos_field][predictFrag] = 0
                '''
                
    def readTrafficData(self):
        #一个一个traffic文件打开        
        for traffic in self.trafficList:
            trafficFile = open(traffic)
            for line in trafficFile:
                district_hash,oneLevel,twoLevel,threeLevel,fourLevel,timePoint = line.split('\t')
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
                pos_field = self.dist_hash2intDic[district_hash]
                self.trafficDic[pos_field][timeFrag] = []
                self.trafficDic[pos_field][timeFrag].extend([int(oneLevel.split(':')[1]), int(twoLevel.split(':')[1]), int(threeLevel.split(':')[1]), int(fourLevel.split(':')[1])])
            trafficFile.close()
        '''print '----:',self.trafficDic['2']['2016-01-30-103']'''     
        return self.trafficDic
        
#获取天气信息
class WeatherFeaturing():
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.weatherPath = cwd + '\\season_2\\test_set_2\\weather_data'
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.weatherDic = {}
        
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.weatherList = GFP.getFilePath(self.weatherPath)
        
        
    def readWeatherData(self):
        weatherTimeFields = set()
        for weather in self.weatherList:
            weatherFile = open(weather)
            for line in weatherFile:
                timePoint,weatherLevel,temperature,pm25 = line.split('\t')
                pm25 = pm25.split('\n')[0]
                Date = timePoint.split()[0]
                Time = timePoint.split()[1]
                timeInfo = Time.split(':')
                hour = int(timeInfo[0])
                minute = int(timeInfo[1])
                time_field = hour*6 + minute/10 + 1
                #timeFrag: 2016-01-24-122
                timeFrag = Date + '-' + str(time_field)
                weatherTimeFields.add(timeFrag)
                self.weatherDic[timeFrag] = []
                self.weatherDic[timeFrag].extend([weatherLevel,temperature,pm25])
            weatherFile.close()
                
        return self.weatherDic,weatherTimeFields
                
        
#组合所有的结果
class AssembleDics():
    def __init__(self):
        TOF = TestOrderFeaturing()
        
        TF = TrafficFeaturing()
        
        WF = WeatherFeaturing()
        
        
        self.IS_HOLIDAY_DIC = IS_HOLIDAY_DIC
        self.RUSH_LIST = RUSH_LIST
        #PF = PoiFeaturing()
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        print 'Starting reading order featuring...'
        self.totalPriceDic,self.requestDic,self.responseDic,self.gapDic,self.transOrders = TOF.readOrderData()
        print 'Finishing order featuring....',time.strftime('%Y-%m-%d %X',time.localtime())        
        self.trafficDic = TF.readTrafficData()
        time.sleep(5)
        print 'Finishing traffic featuring....',time.strftime('%Y-%m-%d %X',time.localtime())
        self.weatherDic,self.weatherTimeFields = WF.readWeatherData()
        print 'Finishing weather featuring...',time.strftime('%Y-%m-%d %X',time.localtime())
        
        self.rowsLi = []
        self.predict_data = PREDICT_DATE
        self.date_to_week = DATE_TO_WEEK
        #得到POI数据
        POI = GetPoiDic()
        self.disFacilityNumDic = POI.getPoiDic()
    
    #该函数用来将上面的到的所有结果组合起来
    def assemble(self):
        count = 0
        #一天一天的处理数据
        for predictDate in self.predict_data:
            for pos_field in range(1,67): 
                for time_field in self.predict_time_fields:                            
                    #得到前三个时间片的交通信息，天气信息，无重复的用户数目，无重复的司机数目，订单数目，平均价格
                    trafficList = []
                    weatherList = []
                    userList = []
                    responseList = []
                    requestList = []
                    orderMeanPriceList = []
                    gapList = []
                    pos_field = str(pos_field)
                    predictGap = 0
                    totalPriceList = []
                    transOrdersList = []
                    '''用来做label使用'''
                    noChangeTimeFrag = predictDate + '-' + str(time_field)
                    
                    date = predictDate.split('-')[2]
                    if date[0] == '0':
                        date = int(date[1])
                    else:
                        date = int(date[0]) * 10 + int(date[1])
                        
                    for i in range(3):
                        time_field -= 1
                        #得到前三个时间片的交通信息
                        timeFrag = predictDate + '-' + str(time_field)
                        try:
                            oneLevel,twoLevel,threeLevel,fourLevel = self.trafficDic[pos_field][timeFrag]
                        except:
                            oneLevel,twoLevel,threeLevel,fourLevel = [0,0,0,0]

                        trafficList.extend([oneLevel,twoLevel,threeLevel,fourLevel])
                        
                        #因为天气信息不够全面，所以每一次的天气都取离当前时间最近的+天气信息
                        if timeFrag in self.weatherTimeFields:
                            try:
                                weatherLevel,temperature,pm25 = self.weatherDic[timeFrag]
                            except Exception,ex:
                                weatherLevel,temperature,pm25 = [0,0,0]
                        #如果该时间片的天气信息没有在给定的天气数据中，那么赋予该时间片最近的时间的天气数据
                        else:
                            tfList = []
                            #tfList: [2016-01-22-122,...]
                            tfList = list(self.weatherTimeFields)
                            tfList.sort()
                            t_f = timeFrag
                            #找出距离当前时间片最近的那个时间frag
                            for i in range(len(tfList)-1):
                                #如果该时间片在给定的天气信息中
                                if timeFrag in tfList:
                                    t_f = timeFrag
                                    break
                                #如果这三个时间片处于一天之中
                                else:
                                    if tfList[i].split('-')[2] == timeFrag.split('-')[2] == tfList[i+1].split('-')[2]:
                                        #比较离哪一个时间片最近
                                        if abs(int(tfList[i].split('-')[3]) - int(timeFrag.split('-')[3])) <= abs(int(tfList[i+1].split('-')[3]) - int(timeFrag.split('-')[3])):
                                            t_f = tfList[i]
                                            break
                                        else:
                                            t_f = tfList[i+1]
                                    #如果三个时间片不处于同一天之中，那么取同一天的那个数据作为参考
                                    elif tfList[i].split('-')[2] == timeFrag and tfList[i+1].split('-')[2] != timeFrag:
                                        t_f = tfList[i]
                                        break
                                    elif tfList[i].split('-')[2] != timeFrag and tfList[i+1].split('-')[2] == timeFrag:
                                        t_f = tfList[i+1]
                                        break

                            weatherLevel,temperature,pm25 = self.weatherDic[t_f]                           
                        weatherList.extend([weatherLevel,temperature,pm25])
                        
                        #得到response数，也就是司机id不为NULL的数目
                        try:
                            response = self.responseDic[pos_field][timeFrag]
                        except:
                            response = 0
                        responseList.append(response)                            
                        
                        #得到订单数目
                        try:
                            requestNum = self.requestDic[pos_field][timeFrag]
                        except:
                            requestNum = 0
                        requestList.append(requestNum) 
                        
                        #得到该时间片的总价格
                        try:
                            totalPrice = self.totalPriceDic[pos_field][timeFrag]
                        except Exception,ex:
                            totalPrice = 0
                        totalPriceList.append(totalPrice)

                        try:
                            meanPrice = totalPrice/requestNum
                        except:
                            meanPrice = 0
                        orderMeanPriceList.append(meanPrice)
                        #计算该时间片该区域中的gap值
                        try:
                            gap = self.gapDic[pos_field][timeFrag]
                        except:
                            gap = 0
                        gapList.append(gap)
                                                
                        try:
                            transOrders = self.transOrders[pos_field][timeFrag]
                        except:
                            transOrders = 0
                        transOrdersList.append(transOrders)
                    
                    '''去掉几个特定的时间片，因为2014-01-25-46和2016-01-29-46这两个时间片是不需要的，因此在计算到其倒数第三个时间片的时候
                       不把计算得到的数据加入到列表中
                    '''
                    
                    if timeFrag == '2016-01-25-43' or timeFrag == '2016-01-29-43':
                        continue                       
                    
                    isHoliday = self.IS_HOLIDAY_DIC[predictDate]
                    frag = int(noChangeTimeFrag.split('-')[-1])
                    if frag in self.RUSH_LIST:
                        isRushHour = 1
                    else:
                        isRushHour = 0
                    week = self.date_to_week[predictDate]
                    if week == 1:
                        isWeek = 0
                    elif week == 5:
                        isWeek = 1
                    else:
                        isWeek = 2                    
                    
                    #判断是否降温
                    if date in [23,24,26,29,30,31]:
                        isWeatherDecrease = 1
                    else:
                        isWeatherDecrease = 0
                        
                    #添加26个poi数据
                    poiList = self.disFacilityNumDic[int(pos_field)]
                    li = [[pos_field,noChangeTimeFrag],trafficList,weatherList,requestList,responseList,gapList,orderMeanPriceList,totalPriceList,poiList,[week],[isHoliday,frag,isRushHour],transOrdersList,[isWeek],[isWeatherDecrease]]
                    tempLi = []
                    #压平嵌套列表
                    [tempLi.extend(i) for i in li]
                    #将获取到的一行数据加入到最终的列表中
                    self.rowsLi.append(tempLi)
                #df = pd.DataFrame(gap_data,columns= ['pos','time','gap'])
        df = pd.DataFrame(self.rowsLi,columns=['pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'requestNum1','requestNum2','requestNum3','responseNum1','responseNum2','responseNum3','gap1','gap2','gap3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week','isHoliday','time_fragment','isRushHour','transOrders1','transOrders2','transOrders3','isWeek','isWeatherDecrease'])
        df.to_csv('testResult2.csv')

                    

def doProcess():
     
    print 'Reading data:',time.strftime('%Y-%m-%d %X',time.localtime())
    ASMB = AssembleDics() 
    #从测试集中读取的以区域作为主键的字典
    #{区域：{时间片:[[订单信息1],[订单信息2]...}...}
    print 'Processing...'
    ASMB.assemble()
    #print '测试集中用户的个数为：',len(userDic)
    print 'Finish processing data:',time.strftime('%Y-%m-%d %X',time.localtime())
    
    #return tof_dist_Dic

if __name__ == "__main__":
    doProcess()