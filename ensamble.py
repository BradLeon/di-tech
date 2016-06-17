# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 18:44:07 2016

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
from sklearn.tree import ExtraTreeRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import make_scorer
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from sklearn import cross_validation
from sklearn.cross_validation import KFold

from datetime import datetime


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
    


def LRprediction(train_X,train_y,test_X,valid_X, valid_y):
    tuned_parameters ={'penalty': 'l1', 'tol': 1e-4,\
                     'C': 1}
    lgr = LogisticRegression(**tuned_parameters)
    
    lgr.fit(train_X,train_y)
    result=lgr.predict(test_X)
    
    valid_ypred =  lgr.predict(valid_X)   
    
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score of LogitRregression in valid set is :' , valid_mape
    return result   
    

def ExtraTreeRegressorPrediction(train_X,train_y,test_X,valid_X, valid_y):
    etr = ExtraTreeRegressor()
    etr.fit(train_X,train_y)
    
    result=etr.predict(test_X)
    
    valid_ypred =  etr.predict(valid_X)   
    
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score of ExtraTreeRegressor in valid set is :' , valid_mape
    return result   
    
    
def LinearRegressionPredict(train_X,train_y,test_X,valid_X,valid_y):
    tuned_parameters ={'normalize': True }
   
    LR = LinearRegression(**tuned_parameters)
    
    LR.fit(train_X,train_y)
    result = LR.predict(test_X)
    
    valid_ypred =  LR.predict(valid_X)   
    
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score of LinearRegressor in valid set is :' , valid_mape
    return result  
 
def RandomForestPredict(train_X,train_y,test_X,valid_X, valid_y):
    
    tuned_parameters ={'n_estimators': 800 }
    
    rf = RandomForestRegressor(**tuned_parameters)
    
    rf.fit(train_X,train_y)
    result = rf.predict(test_X)
    
    valid_ypred =  rf.predict(valid_X)   
    
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score of RandomForest in valid set is :' , valid_mape
    return result  
    
  
class Ensemble(object):
    def __init__(self, n_folds, stacker, base_models):
        self.n_folds = n_folds
        self.stacker = stacker
        self.base_models = base_models

    def fit_predict(self, X, y,T):
        X = np.array(X)
        y = np.array(y)
        T = np.array(T)
        #valid_X = np.array(valid_X)
        #valid_y = np.array(valid_y)

        folds = list(KFold(len(y), n_folds=self.n_folds, shuffle=True, random_state=2016))

        S_train = np.zeros((X.shape[0], len(self.base_models)))
        S_test = np.zeros((T.shape[0], len(self.base_models)))


        for i, clf in enumerate(self.base_models):
            
            print 'ensamble step i:', i , datetime.now()
            S_test_i = np.zeros((T.shape[0], len(folds)))

            for j, (train_idx, test_idx) in enumerate(folds):
                print 'ensamble j:' , j ,datetime.now()
                X_train = X[train_idx]
                y_train = y[train_idx]
                X_holdout = X[test_idx]
                # y_holdout = y[test_idx]
                clf.fit(X_train, y_train)
                y_pred = clf.predict(X_holdout)[:]
                S_train[test_idx, i] = y_pred
                S_test_i[:, j] = clf.predict(T)[:]
 
            S_test[:, i] = S_test_i.mean(1)
            #S_valid[:,i] = S_valid_i.mean(1)
            
        
        self.stacker.fit(S_train, y)
        y_pred = self.stacker.predict(S_test)[:]
        
        joblib.dump(self.stacker,'stacker_gbrt.model')
        
        return y_pred
    

if __name__=='__main__':
    
    train=pd.read_csv('D:\\di_tech\\dataset\\season_1\\trainResult0611_binary.csv',header=0)
    #train2=pd.read_csv('D:\\di_tech\\dataset\\season_1\\train_downupSample2.csv',header=0)
   # validation=pd.read_csv('D:\\di_tech\\dataset\\season_1\\validation_Result.csv',header=0)
    test=pd.read_csv('D:\\di_tech\\dataset\\season_1\\testResult_season2_binary.csv',header=0)
    
    
    train_X = train.values[:,4:]
    #ÑµÁ·¼¯µÄtargetÁÐ
    train_y = train['predictGap'].values
    

    test_X = test.values[:,3:]
    
    print 'step1:' ,datetime.now()
    
      
    train_X, valid_X , train_y, valid_y = cross_validation.train_test_split(train_X, train_y,test_size=0.3,random_state=0)
    '''
    print 'step2:' ,datetime.now()
    result_lgr = LRprediction(train_X,train_y,test_X,valid_X, valid_y)
    print 'step3:' ,datetime.now()
    result_etr = ExtraTreeRegressorPrediction(train_X,train_y,test_X,valid_X, valid_y)
    print 'step4:' ,datetime.now()
    result_lr = LinearRegressionPredict(train_X,train_y,test_X,valid_X,valid_y)
    
    print 'step5:' ,datetime.now()
    result_rf = RandomForestPredict(train_X,train_y,test_X,valid_X, valid_y)
    print 'step6:' ,datetime.now()
    
     
    submission = pd.DataFrame({'pos':test.pos,'time':test.timeFrag,'lgr':result_lgr,'etr':result_etr,'lr':result_lr,'rf':result_rf}, columns=['pos','time','lgr','etr','lr','rf'])
    submission.to_csv("D:\\di_tech\\dataset\\season_1\\result\\submission_ensamble.csv",index=False)
    '''
    base_models=[]
    tuned_parameters1 ={'penalty': 'l1', 'tol': 1e-4,\
                     'C': 1}
    lgr = LogisticRegression(**tuned_parameters1)    
    etr = ExtraTreeRegressor()
    tuned_parameters2={'normalize': True }   
    lr = LinearRegression(**tuned_parameters2)    
    rf = RandomForestRegressor(n_estimators=800)    
    base_models=[lgr,etr,lr,rf]
    
    exp_parameters ={'learning_rate':0.0005 ,  'n_estimators': 1200,\
                        'max_depth':7, 'subsample': 0.7 ,'loss': 'huber' }
    
    gbrt =  GradientBoostingRegressor(**exp_parameters) 
    print 'step2:' ,datetime.now()
    
    ensamble = Ensemble(5,gbrt,base_models)
    
    valid_preds = ensamble.fit_predict(train_X,train_y,valid_X)
    loss = mape_loss(valid_y , valid_preds)
    print 'mape of ensamble :' , loss
    
   
    
    submission = pd.DataFrame({'true': valid_y ,'pred':valid_preds }, columns=['true' ,'pred'])
    submission.to_csv("D:\\di_tech\\dataset\\season_1\\result\\valid_ensambel.csv",index=False)
    print 'step3:' ,datetime.now()
