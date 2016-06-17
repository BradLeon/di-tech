# -*- coding: utf-8 -*-

'''
该文件用来生成区域统计信息字典 tof_dist_Dic{}
用户订单信息字典userDic{}
区域请求字典time_field_request{}
区域响应字典time_field_response{}
拥堵字典trafficDic{}
poi字典poiDic = {}
顾客数usersDic = {}
司机数driversDic = {}
gap数self.gapDic = {}

时间点 2016-01-22-122  以timeFrag标识

userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
tof_dist_Dic = {区域：{时间片:该时间片的有响应的订单的总价格)}...}
time_field_request{} = {区域1：{时间片1：该时间片的请求数目}，{时间片2：该时间片的请求数目}...}})
time_field_response{} = {区域2：{时间片1：该时间片的响应数目}，{时间片2：该时间片的响应数目}...}
trafficDic = {区域1：{时间片：[231,33,13,10]}，...}
disFacilityNumDic = {区域1：[POI类目及数量信息],...}
usersDic = {区域2：{时间片1：顾客id_[]}}
driversDic = {区域1：{时间片1：司机id_[]}，...}
gapDic = {区域1：{时间片1：gap, 时间片2：gap}，...}
'''

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

'''将日期与星期几对应起来'''
dates = ['2016-01-22','2016-01-24','2016-01-26','2016-01-28','2016-01-30']
week = [5,7,2,4,6]
DATE_TO_WEEK = dict(zip(dates,week))

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
        self.usersDic = {}
        self.driversDic = {}
        self.userDic = {}
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
                 
                 self.tof_dist_Dic[pos_field] = {}
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
                    if len(self.tof_dist_Dic[pos_field][timeFrag]) >= 0:
                        self.tof_dist_Dic[pos_field][timeFrag].append(Price)
                except:
                    self.tof_dist_Dic[pos_field][timeFrag] = []
                    self.tof_dist_Dic[pos_field][timeFrag].append(Price)
                        
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

            orderFile.close()
        
        '''test1 
        pp = 0
        for i in self.tof_dist_Dic['7']['2016-01-22-104']:
            pp += float(i)
        print 'pp: self.tof_dist_Dic  7   2016-01-22-104---->  ',pp
        '''        
        
        
        
        #print len(self.noRepeatUsersDic['2']['2016-01-22-92']),'____________+++++len(self.noRepeatUsersDic 2016-01-22-92'
        #print self.time_field_response,'-----------------------'                    
        return self.tof_dist_Dic,self.userDic,self.time_field_request,self.time_field_response,self.usersDic,self.driversDic,self.gapDic             
        
#获取拥堵信息
class TrafficFeaturing():
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.trafficPath = cwd + '\\test_set_1\\traffic_data'
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
        self.weatherPath = cwd + '\\test_set_1\\weather_data'
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
        self.demand_time_fields= DEMAND_TIME_FIELDS
        self.predict_time_fields= PREDICT_TIME_FIELDS
        self.tof_dist_Dic,self.userDic,self.time_field_request,self.time_field_response,self.usersDic,self.driversDic,self.gapDic = TOF.readOrderData()
        self.trafficDic = TF.readTrafficData()
        self.weatherDic,self.weatherTimeFields = WF.readWeatherData()
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
                    driverList = []
                    orderNumList = []
                    orderMeanPriceList = []
                    gapList = []
                    pos_field = str(pos_field)
                    predictGap = 0
                    totalPriceList = []
                    #保存要预测的时间片
                    noChangeTimeFrag = predictDate + '-' + str(time_field)
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
                            t_f = ' '
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
                        '''得到该区域不重复的用户数目和该区域不重复的司机数目'''
                        #print '__***:',timeFrag,'--pos_field:',pos_field
                        '''有些区域某个时间片是没有用户的，比如第三区域 2016-01-22-92没有用户'''
                        try:
                            usersNum = len(self.usersDic[pos_field][timeFrag])
                        except:
                            usersNum = 0
                            #print 'noRepeatUsersNum:',pos_field,timeFrag
                            count += 1
                        try:
                            driversNum = len(self.driversDic[pos_field][timeFrag])
                        except:
                            driversNum = 0
                            #print 'noRepeatDriversNum:',pos_field,timeFrag
                            count += 1
                        userList.append(usersNum)
                        driverList.append(driversNum)
                        #得到订单数目
                        try:
                            orderNum = self.time_field_response[pos_field][timeFrag]
                        except:
                            orderNum = 0
                        orderNumList.append(orderNum) 
                        #得到该时间片的总价格
                        totalPrice = 0.0
                        try:
                            for price in self.tof_dist_Dic[pos_field][timeFrag]:
                                totalPrice += float(price)
                        except:
                            totalPrice += 0
                        totalPriceList.append(totalPrice)
                        #print 'totalPrice:',totalPrice
                        try:
                            meanPrice = totalPrice/orderNum
                        except:
                            meanPrice = 0
                        orderMeanPriceList.append(meanPrice)
                        #计算该时间片该区域中的gap值
                        try:
                            gap = self.gapList[pos_field][timeFrag]
                        except:
                            gap = 0
                        gapList.append(gap)
                    
                    
                    '''去掉几个特定的时间片，因为2014-01-24-46和2016-01-28-46这两个时间片是不需要的，因此在计算到其倒数第三个时间片的时候
                       不把计算得到的数据加入到列表中
                    '''
                    if timeFrag == '2016-01-24-43' or timeFrag == '2016-01-28-43':
                        continue 
                    
                    
                    '''test1
                    if noChangeTimeFrag == '2016-01-22-106' and pos_field == '7':
                        print '+++___***: ',totalPriceList
                    '''
                    
                    week = self.date_to_week[predictDate]
                    #添加26个poi数据
                    poiList = self.disFacilityNumDic[int(pos_field)]
                    li = [[pos_field,noChangeTimeFrag],trafficList,weatherList,userList,driverList,orderNumList,orderMeanPriceList,totalPriceList,gapList,poiList,[week]]
                    tempLi = []
                    '''压平嵌套列表'''
                    [tempLi.extend(i) for i in li]
                    #将获取到的一行数据加入到最终的列表中
                    self.rowsLi.append(tempLi)
                #df = pd.DataFrame(gap_data,columns= ['pos','time','gap'])
        df = pd.DataFrame(self.rowsLi,columns=['pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'userNum1','userNum2','userNum3','driverNum1','driverNum2','driverNum3','orderNum1','orderNum2','orderNum3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','gap1','gap2','gap3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week'])
        df.describe()
        df.to_csv('testResult.csv')
        print '某时间片没有人或司机的总数为：',count

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