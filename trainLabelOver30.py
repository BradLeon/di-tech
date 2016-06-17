# -*- coding: utf-8 -*-
'''
本程序用来将训练集分成label大于30 和label小于30的两个数据集
'''
import os
from pandas import Series,DataFrame
import pandas as pd


trainPath = os.getcwd() + '\\trainResult.csv'
trainFile = file(trainPath,'rb')
over30List = []
below30List = []
count = 0
for line in trainFile:
    if count !=0:
        li = []
        for item in line.strip('\r\n').split(',')[1:]:
            li.append(item)
        try:
            if int(li[0]) >= 30:
                over30List.append(li)
            else:
                below30List.append(li)
        except Exception,ex:
            print Exception,'---->',ex
        if count == 5:
            print li
    count += 1
trainFile.close()
dfOver30 = pd.DataFrame(over30List,columns=['predictGap','pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'userNum1','userNum2','userNum3','driverNum1','driverNum2','driverNum3','orderNum1','orderNum2','orderNum3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','gap1','gap2','gap3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week'])
dfOver30.to_csv('over30TrainResult.csv')

dfBelow30 = pd.DataFrame(below30List,columns=['predictGap','pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'userNum1','userNum2','userNum3','driverNum1','driverNum2','driverNum3','orderNum1','orderNum2','orderNum3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','gap1','gap2','gap3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week'])
dfBelow30.to_csv('below30TrainResult.csv')


        
        
        
        