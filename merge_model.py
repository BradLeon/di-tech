# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 11:18:13 2016

@author: Leon
"""


import numpy as np
import pandas as pd
import time
import csv
from sklearn.grid_search import GridSearchCV
from sklearn.grid_search import RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import make_scorer
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from sklearn import cross_validation
import xgboost as xgb

from datetime import datetime
import pickle

def MAPE_score(y ,ypre):
    
    if abs(y-0.0)<0.00001:
        error = 0.0
    else:
        error = float(abs(y-ypre))/y
        
    return error

#labels ,preds is array or list like
def mape_loss(labels,preds):
    
    if len(labels)!=len(preds):
        print 'there is error, that prediction number  is not equal to samples'
    else:
        Sum = 0.0
        for i in range(len(labels)):
            Sum += MAPE_score(labels[i], preds[i])
        return float(Sum)/len(labels)
    
    

if __name__ == '__main__':

    train=pd.read_csv('/home/lael/Documents/trainResult0611_binary.csv',header=0)
   # validation=pd.read_csv('D:\\di_tech\\dataset\\season_1\\validation_Result.csv',header=0)
    test=pd.read_csv('/home/lael/Documents/testResult_season2_binary.csv',header=0)
    
    #print(train.info())
    
    #Ç°Á½ÁÐÎªuser_id item_id£¬²»¼ÓÈëÑµÁ·
    #print 'train:',train.values
    
    #print 'test:', test.values
    
    train_X = train.values[:,4:]
    #ÑµÁ·¼¯µÄtargetÁÐ
    train_y = train['predictGap'].values
    
   # valid_X = validation.values[:,4:]
    #ÑµÁ·¼¯µÄtargetÁÐ
   # valid_y = validation['predictGap'].values    
              
    #¹¹Ôì²âÊÔ¼¯£¬Ç°Á½ÁÐ²»¼ÓÈëÑµÁ·
    test_X = test.values[:,3:]
    
    print 'step1:' ,datetime.now()
    
   
    #features_list = train.columns.values[4:]
   # important_idx,sorted_idx=gbdtForFeature(train_X,train_y,features_list)
    #train_X = train_X[:, important_idx][:, sorted_idx];print(str(train_X.shape[1])+" features")
    #test_X = test_X[:, important_idx][:, sorted_idx]    
    
    
    train_X, valid_X , train_y, valid_y = cross_validation.train_test_split(train_X, train_y,test_size=0.3,random_state=0)
    
    print 'step2:' , datetime.now()

    #×öÌØÕ÷Ñ¡Ôñ,²¢È¥µôÌØÕ÷¼¯ÖÐ²»tÖØÒªµÄÌØÕ÷
    '''
    features_list = train.columns.values[:-1]
    train_X = train_X[:, important_idx][:, sorted_idx];print(str(train_X.shape[1])+" features")
    test_X = test_X[:, important_idx][:, sorted_idx]
    '''
    
    
    normal_model = joblib.load('gbrt_normal_1.model')
    f = open('gbrt_up01.txt','r')
    s=f.read()
    
    up_model = pickle.loads(s)
    #up_model2 = joblib.load('gbrt_up3.model')

    dtrain = xgb.DMatrix(data = train_X,label=train_y )
    dtest = xgb.DMatrix(data = test_X)
    dvalid = xgb.DMatrix(data=valid_X, label=valid_y)
    
    pred_y1 = normal_model.predict(dvalid)
    pred_y2 = up_model.predict(dvalid)
    #pred_y3 = up_model2.predict(valid_X)
        
    pred_final = []
    for i in range(len(valid_y)):
        if pred_y2[i]>20 :
            pred_final.append(pred_y2[i])
        else:
            pred_final.append(pred_y1[i])
    pred_final = np.array(pred_final)
    
    pred_final2= pred_final
    pred_final2[pred_final<1] =1.0
    
    pred_final3=[]
    for item in pred_final2:
        gap = float(round(item))
        pred_final3.append(gap)
    pred_final3 = np.array(pred_final3)
    
    valid_mape = mape_loss( valid_y,pred_final)
    valid_mape2 = mape_loss(valid_y,pred_y1)
    valid_mape3 = mape_loss( valid_y,pred_y2)
    valid_mape4 = mape_loss( valid_y,pred_final2)
    valid_mape5 = mape_loss( valid_y,pred_final3)
    
    
    print ' the mape score in valid set is :' , valid_mape ,valid_mape2,valid_mape3
    print ' the mape score in valid set after round is :' , valid_mape ,valid_mape4,valid_mape5
    
    
    result1 = normal_model.predict(dtest)
    result2 = up_model.predict(dtest)
    
    result_final = []
    for i in range(len(result1)):
        if result2[i]>20:
            result_final.append(result2[i])
        else:
            result_final.append(result1[i])
    result_final = np.array(result_final)
    

    #ÓÃÂß¼­»Ø¹é×öÑµÁ·²¢Ô¤²â
    #result = gbrtprediction2(train_X,train_y,test_X,valid_X, valid_y)
    
    #result = LRprediction2(train_X,train_y,test_X,valid_X, valid_y)
 
    print(type(result_final))
    #print(result)
    print 'step5:' , datetime.now()
    
    submission = pd.DataFrame({'pos':test.pos,'time':test.timeFrag,'gap':result1}, columns=['pos','time','gap'])
    submission.to_csv("/home/lael/Documents/submission_gbrt_s2_3.csv",index=False)
    