# -*- coding: utf-8 -*-

from pandas import Series,DataFrame
import pandas as pd
import time
import os
from datetime import datetime

class GetUserInfo():
    def readData(self):
        userDic = {}
        count = 0
        li = []
        #得到训练集中的所有文件名字列表
        trainList = self.getFilePath()  
        orderList = trainList[1]        
 
        '''
        从路径中一行一行的读取数据，将每一行的数据按照用户的id加入到相应的字典中
        例如：userDic = {'u1':[[u1的第一个订单信息],[u1的第二个订单信息],[u1的第三个订单信息]...], 'u2':[[],[]...], ...}
        '''
        orderDir = os.getcwd() + '\\training_data\\order_data\\'
        lineCount = 0
        startEndSame = 0
        orderSet= set()
        for order in orderList:
            order = orderDir + order
            for line in open(order):
                lineCount += 1
                order_id,driver_id,passenger_id,start_district_hash,dest_district_hash,Price,timePoint= line.split('\t')
                #统计起点和终点重复的订单数目
                if start_district_hash == dest_district_hash:
                    startEndSame += 1
                #Date:2016-01-01
                Date = timePoint.split()[0]
                #Time:11:26:25
                Time = timePoint.split()[1]
                
                timeInfo = Time.split(':')
                hour = int(timeInfo[0])
                minute = int(timeInfo[1])
                frag = hour*6 + minute/10 + 1
                timeFrag = Date + '-' + str(frag)                

                '''
                time = datetime.strptime(Date + ' ' + Time, '%Y-%m-%d %H:%M:%S')
                timeFrag = (time.hour*60 + time.minute + 1)/10
                '''
                if order_id not in orderSet:
                    #如果该用户还未读取进来
                    if passenger_id not in userDic:
                        userDic[passenger_id] = [[order_id,driver_id,start_district_hash,dest_district_hash,Price,Date,Time,timeFrag]]
                    else:
                        userDic[passenger_id].append([order_id,driver_id,start_district_hash,dest_district_hash,Price,Date,Time,timeFrag])
                    orderSet.add(order_id)
                    
        userOrderNum = {}
        
        for key,value in userDic.items():
            try:
                userOrderNum[len(value)] += 1 
            except Exception,ex:
                userOrderNum[len(value)] = 1
            if len(value) == 104:
                print '-----------------------------'
                print key,userDic[key][0]
                print '-----------------------------'
                
        print '******************************************************'
        print userOrderNum
        print '训练集中总的订单数目有：',lineCount
        print '训练集中总的订单起点和终点数目相同的有：',startEndSame
        print '训练集中总的订单重复条目为：',lineCount - len(orderSet)
        print '训练集中总的订单不重复的条目有：', len(orderSet)
        print '******************************************************'
        return userDic
        
    '''
    获取某一个文件夹下面的所有文件的Path,存储为list,用于后面的读取数据    
    '''
    def getFilePath(self):
        #得到当前的目录
        cwd = os.getcwd()
        orderPath = cwd + '\\training_data'
        trainList = []
        
        #从路径  C:\pyWorkspace\DIDI\training_data 下读取文件，该路径和当前目录cwd有关
        for root,dirs,files in os.walk(orderPath):
            if len(files) != 0:
                trainList.append(files)
        return trainList



def doProcess():
    GUI = GetUserInfo()  
    print 'Reading order data:',time.strftime('%Y-%m-%d %X',time.localtime())
    userDic = GUI.readData()
    print '在训练集中的所有订单中的不同用户总数为：',len(userDic)
    print 'Finish reading order data:',time.strftime('%Y-%m-%d %X',time.localtime())


if __name__ == "__main__":
    doProcess()