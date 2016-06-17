# -*- coding: utf-8 -*-
"""
Created on Fri Jun 03 10:22:32 2016

@author: Leon
"""

'''
xgboost - version1
'''


import numpy as np
import xgboost as xgb
import pandas as pd
import math
from datetime import datetime
import sklearn 
from sklearn import cross_validation
from sklearn.externals import joblib


TRAIN_SET = '/home/lael/Documents/trainResult0611_binary.csv'
TEST_SET = '/home/lael/Documents/testResult_season2_binary.csv'
RESULT_FILE= '/home/lael/Documents/result0611_2.csv'




#  loss function L( y, ^y) = abs(y-^y)/y
def obj1(preds ,dtrain):
    labels = dtrain.get_label()
    grad = np.arange(float(len(labels)))
    hess = np.zeros(len(labels),dtype=float)
    
    grad[labels == 0] = 0
    grad[labels>preds] = float(-1)/labels[labels>preds]
    grad[labels<=preds] = float(1)/labels[labels<=preds]

    return grad,hess
    
#  L(y, ^y) = 1/2*(y - ^y)^2 /y
def obj2(preds, dtrain):
    labels = dtrain.get_label()
    grad = np.arange(float(len(labels)))
    
    grad = (preds - labels)/labels    
    grad[labels==0] = 0
    
   # grad[labels!=0] = float(preds[labels!=0] - labels[labels!=0]) / labels[labels!=0]
    hess = float(1)/ labels
    return grad , hess

#  L(y,^y) = exp[abs( y - ^y)/y]
def obj3(preds , dtrain):
    labels = dtrain.get_label()
    grad = np.arange(float(len(labels)))
    hess = np.arange(float(len(labels)))
    grad[labels ==0] = 0
    hess[labels==0] = 0
    for i in range(len(labels)):
        if labels[i]!=0:
            if labels[i] > preds[i]:
                grad[i] = float(-1)/labels[i] * math.exp(float(labels[i]-preds[i])/labels[i])
            else:
                grad[i] = float(1)/labels[i] * math.exp(float(labels[i]-preds[i])/labels[i])
            hess[i] = float(1)/math.pow(labels[i],2) * math.exp(float(labels[i]-preds[i])/labels[i])
    return grad , hess

def logregobj(preds, dtrain):
    labels = dtrain.get_label()
    preds = 1.0 / (1.0 + np.exp(-preds))
    grad = preds - labels
    hess = preds * (1.0-preds)
    return grad, hess   

def evalMAPE(preds , dtrain):
    labels = dtrain.get_label()
    #Sum = 0.0
    #print 'preds:' , preds
    #print 'labels:',labels
    
    diff = abs(preds - labels)/labels
   # print 'diff:',diff
    
    diff[labels==0] = 0
    #print 'diff2', diff
    
    return 'MAPE ', float(sum(diff))/len(labels)
    #for i in range(len(labels)):
     #   if abs(labels[i] - 0)< 0.00001:
      #      err = 0.0
       # else:
       #     err = float(abs(labels[i]-preds[i]))/labels[i]
       # Sum += err
       # print Sum
    #return 'MAPE' , float(Sum)/len(labels)        




def XGBRegressorPred(params , dtrain ,dvalid, dtest, watch_list ,num_round):
    
    print 'step2:' , datetime.now()
    gbt = xgb.train(params , dtrain=dtrain, obj=obj3, feval=evalMAPE, num_boost_round =num_round) 
    valid_y_pred = gbt.predict(dvalid)
    '''
    new_valid_ypred = []
    valid_y_pred[valid_y_pred<1]= 1.0
    for item in valid_y_pred:
        item2=float(round(item))
        new_valid_ypred.append(item2)
    new_valid_ypred=np.array(new_valid_ypred)
    #valid_mape = mape_loss(valid_y,new_valid_ypred)
    '''
    valid_y = dvalid.get_label()
    #for i in range(len(valid_y)):
        
     #   print 'pred y & true y', valid_y_pred[i] , valid_y[i]
    
    print 'valid mape score:', evalMAPE(valid_y_pred,dvalid)
    
    joblib.dump(gbt, 'gbrt_normal_1.model')
    
    '''
    gbt.save_model('gbdt01.model')
    gbt.dump_model('gbdt01.raw.txt')
    '''
    print 'step3:' ,datetime.now()
        
    result = gbt.predict(dtest)  
    
    return result

  


if __name__ == "__main__":
    

    
    train=pd.read_csv(TRAIN_SET,header=0)
    test=pd.read_csv(TEST_SET,header=0)
    
   
    train_X = train.values[:,4:]
    train_y = train['predictGap'].values
      
    test_X = test.values[:,3:]
    

    
    
    #features_list = train.columns.values[2:-1]
    #important_idx,sorted_idx=gbdtForFeature(train_X,train_y,features_list)
   # train_X = train_X[:, important_idx][:, sorted_idx];print(str(train_X.shape[1])+" features")
    #test_X = test_X[:, important_idx][:, sorted_idx]    
       
    train_X, valid_X , train_y, valid_y = cross_validation.train_test_split(train_X, train_y,test_size=0.3,random_state=0)
     
    print  'step1:' ,datetime.now()
    # Transform data format
    dtrain = xgb.DMatrix(data = train_X,label=train_y )
    dtest = xgb.DMatrix(data = test_X)
    dvalid = xgb.DMatrix(data=valid_X, label=valid_y)
    params = { 'booster':'gbtree',  
              'subsample': 0.7,
              'colsample_bytree':0.6,
              'max_depth' :7,
              'eta':0.001 ,
              'silent' :True }
              
    num_round = 1600
    watch_list = [(dvalid,'eval'),(dtrain,'train')]
    print 'params:' , params , num_round
    
    result = XGBRegressorPred(params , dtrain ,dvalid, dtest, watch_list ,num_round)


    
    submission = pd.DataFrame({'pos':test.pos,'time':test.timeFrag,'gap':result}, columns=['pos','time','gap'])           
    submission.to_csv(RESULT_FILE,index=False)    
    
    print 'step4:' ,datetime.now()


    