# -*- coding: utf-8 -*-
'''
该文件用来将训练集拆分成新的训练集和验证集2016/6/8 14:46
'''

import csv
import os
from pandas import Series,DataFrame
import pandas as pd

VALIDATION_FIELDS= [46,58,70,82,94,106,118,130,142]

trainPath = os.getcwd() + '\\trainResult.csv'
trainFile = file(trainPath,'rb')
trainList = []
validationList = []
count = 0
for line in trainFile:
    if count !=0:
        li = []
        for item in line.strip('\r\n').split(',')[1:]:
            li.append(item)
        try:
            if int(li[2].split('-')[3]) in VALIDATION_FIELDS:
                validationList.append(li)
            else:
                trainList.append(li)
        except Exception,ex:
            print Exception,'---->',ex
    count += 1
trainFile.close()
validationDF = pd.DataFrame(validationList,columns=['predictGap','pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'userNum1','userNum2','userNum3','driverNum1','driverNum2','driverNum3','orderNum1','orderNum2','orderNum3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','gap1','gap2','gap3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week'])
validationDF.to_csv('validation_Result.csv')

trainDF = pd.DataFrame(trainList,columns=['predictGap','pos','timeFrag','traffic1_1','traffic1_2','traffic1_3','traffic1_4','traffic2_1','traffic2_2','traffic2_3','traffic2_4',\
                                          'traffic3_1','traffic3_2','traffic3_3','traffic3_4','w1_weatherLevel','w1_temperature','w1_pm2.5',\
                                          'w2_weatherLevel','w2_temperature','w2_pm2.5','w3_weatherLevel','w3_temperature','w3_pm2.5',\
                                          'userNum1','userNum2','userNum3','driverNum1','driverNum2','driverNum3','orderNum1','orderNum2','orderNum3',\
                                          'meanPrice1','meanPrice2','meanPrice3','totalPrice1','totalPrice2','totalPrice3','gap1','gap2','gap3','facilityNum1','facilityNum2','facilityNum3','facilityNum4',\
                                          'facilityNum5','facilityNum6','facilityNum7','facilityNum8','facilityNum9','facilityNum10','facilityNum11','facilityNum12',\
                                          'facilityNum13','facilityNum14','facilityNum15','facilityNum16','facilityNum17','facilityNum18','facilityNum19','facilityNum20',\
                                          'facilityNum21','facilityNum22','facilityNum23','facilityNum24','facilityNum25','facilityTotalNum','date2week'])
trainDF.to_csv('train_Result.csv')


        
        
        
        