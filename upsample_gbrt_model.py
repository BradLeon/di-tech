# -*- coding: utf-8 -*-
"""
Created on Sun May 29 00:59:52 2016

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

from datetime import datetime

#使用随机森林计算特征重要性，进行特征选择。
#参数X：训练集特征 参数y：训练集target  features_list:训练集特征名称列表
def rfForFeature(X,y,features_list):
    #使用默认参数构造随机森林分类器
    forest = RandomForestClassifier(oob_score=True, n_estimators=1000)
    #模型训练
    forest.fit(X, y)
    #获取训练后的特征重要性列表
    feature_importance = forest.feature_importances_
    
    #以特征值重要性的最大值为标准，记为100,衡量其他特征的重要性
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    #print "Feature importances:\n", feature_importance
    #设置特征重要性的阈值为3
    fi_threshold = 3
    #选择重要性大于阈值的特征
    important_idx = np.where(feature_importance > fi_threshold)[0]
    #print "Indices of most important features:\n", important_idx
    #获取特征名称
    important_features = features_list[important_idx]
    print("\n"+str(important_features.shape[0])+"Important features(>"+ str(fi_threshold)+ "% of max importance)...\n")#, \
            #important_features
    #特征按重要性排序
    sorted_idx = np.argsort(feature_importance[important_idx])[::-1]
    
    # 画图表示特征重要性
    pos = np.arange(sorted_idx.shape[0]) + .5
    plt.subplot(1, 2, 2)
    plt.barh(pos, feature_importance[important_idx][sorted_idx[::-1]], align='center')
    plt.yticks(pos, important_features[sorted_idx[::-1]])
    plt.xlabel('Relative Importance')
    plt.title('Variable Importance')
    plt.draw()
    plt.show()
    return important_idx,sorted_idx


def gbdtForFeature(X,y,features_list):
    #使用默认参数构造随机森林分类器
    gbdt = GradientBoostingRegressor(loss='huber',n_estimators=800)
    #模型训练
    gbdt.fit(X, y)
    #获取训练后的特征重要性列表
    feature_importance = gbdt.feature_importances_
    
    #以特征值重要性的最大值为标准，记为100,衡量其他特征的重要性
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    #print "Feature importances:\n", feature_importance
    #设置特征重要性的阈值为3
    fi_threshold = 1
    #选择重要性大于阈值的特征
    important_idx = np.where(feature_importance > fi_threshold)[0]
    #print "Indices of most important features:\n", important_idx
    #获取特征名称
    important_features = features_list[important_idx]
    print("\n"+str(important_features.shape[0])+"Important features(>"+ str(fi_threshold)+ "% of max importance)...\n")#, \
            #important_features
    #特征按重要性排序
    sorted_idx = np.argsort(feature_importance[important_idx])[::-1]
    
    # 画图表示特征重要性
    pos = np.arange(sorted_idx.shape[0]) + .5
    plt.subplot(1, 2, 2)
    plt.barh(pos, feature_importance[important_idx][sorted_idx[::-1]], align='center')
    plt.yticks(pos, important_features[sorted_idx[::-1]])
    plt.xlabel('Relative Importance')
    plt.title('Variable Importance')
    plt.draw()
    plt.show()
    return important_idx,sorted_idx  
    
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
    
    
def gbrtprediction(train_X,train_y,test_X , valid_X, valid_y):
    '''
        使用pipeline构造参数范围，用GridSearch选取最优参数，一般来讲penalty应该是l2，loss_func为log函数，C是l1的权重约束。
    class_weight设为‘auto’的话，基本会使最后的训练结果正负比为1：10，假如'class_weight' :{ 0: 1.0 , 1 : 10.0}是在表明
        训练样本的正负比为1:10，所以将正样本的权重设为10，有平衡样本正负比例的功能。
    '''
    '''
    tuned_parameters =[{'penalty': ['l1'], 'tol': [1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000], 'class_weight':'auto'},\
                    {'penalty': ['l2'], 'tol':[1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000],'class_weight':'auto'}]
    '''
    #tuned_parameters =[{'penalty': ['l2'], 'tol': [1e-3, 1e-4],\
    #                 'C': [0.001, 0.01, 0.1, 1, 10, 100, 1000]}]
                     
    tuned_parameters =[{'learning_rate':[0.05,0.01,0.001, 0.005, 0.0001],  'n_estimators': [400,500,600],\
                        'max_depth' :[5,6,7,8] , 'subsample': [1.0,0.9,0.8,0.7] ,'max_features':[None,'auto','sqrt']}]
    
    print 'step3:' , datetime.now()
    
    mape_scorer = make_scorer(mape_loss ,greater_is_better = False)
    #求最佳参数=
    clf =GridSearchCV(GradientBoostingRegressor(), tuned_parameters, cv=5, scoring=mape_scorer)
    
    clf.fit(train_X,train_y)
    print 'step4 , gridsearch finished' , datetime.now()
    #获得最佳f1分值
    print("    score: %0.3f" % clf.best_score_)
    #获得最佳参数
    best_parameters = clf.best_estimator_.get_params()  
    for param_name in sorted(best_parameters.keys()):  
        print("\t%s: %r" % (param_name, best_parameters[param_name]))
    
    print("Generating GradientBoostingRegressor model with parameters: ")
    #使用最佳参数做驯良
    gbrt =  GradientBoostingRegressor(**best_parameters)  
    
    #使用最佳参数画出learning curve曲线，判断是否过拟合
    #title = "RandomForestClassifier with hyperparams: "+str(best_parameters)
   # midpoint, diff = learningcurve.plot_learning_curve(lgr, title, train_X, train_y, (0.6, 1.01), cv=8, n_jobs=-1, plot=True)
  
    #a nparray result
    gbrt.fit(train_X,train_y)
    valid_ypred =  gbrt.predict(valid_X)   
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score in valid set is :' , valid_mape
    #预测
    result=gbrt.predict(test_X)
    return result 

def gbrtprediction2(train_X,train_y,test_X , valid_X, valid_y):
    '''
        经验参数
    '''
    '''
    tuned_parameters =[{'penalty': ['l1'], 'tol': [1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000], 'class_weight':'auto'},\
                    {'penalty': ['l2'], 'tol':[1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000],'class_weight':'auto'}]
    '''
    #tuned_parameters =[{'penalty': ['l2'], 'tol': [1e-3, 1e-4],\
    #                 'C': [0.001, 0.01, 0.1, 1, 10, 100, 1000]}]
    mape_scorer = make_scorer(mape_loss ,greater_is_better = False)
                     
    exp_parameters ={'learning_rate':0.001 ,  'n_estimators': 2000,\
                        'max_depth':9, 'subsample': 0.7 ,'loss': 'huber'}
    
    print 'prams:' , exp_parameters
    
    #mape_scorer = make_scorer(mape_loss ,greater_is_better = False)
    #求最佳参数=
    #clf =GridSearchCV(GradientBoostingRegressor(), tuned_parameters, cv=5, scoring=mape_scorer)
    #clf.fit(train_X,train_y)
    #获得最佳f1分值
   # print("    score: %0.3f" % clf.best_score_)
    #获得最佳参数
    #best_parameters = clf.best_estimator_.get_params()  
    #for param_name in sorted(best_parameters.keys()):  
   #     print("\t%s: %r" % (param_name, best_parameters[param_name]))
    
    print("Generating LogisticRegressionClassifier model with parameters: ")
    #使用最佳参数做驯良
    gbrt =  GradientBoostingRegressor(**exp_parameters)  
    
    #使用最佳参数画出learning curve曲线，判断是否过拟合
    #title = "RandomForestClassifier with hyperparams: "+str(best_parameters)
   # midpoint, diff = learningcurve.plot_learning_curve(lgr, title, train_X, train_y, (0.6, 1.01), cv=8, n_jobs=-1, plot=True)
  
    #a nparray result
    gbrt.fit(train_X,train_y) 
    valid_ypred =  gbrt.predict(valid_X)
    '''
    new_valid_ypred = []
    valid_ypred[valid_ypred<1]= 1.0
    for item in valid_ypred:
        item2=float(round(item))
        new_valid_ypred.append(item2)
    new_valid_ypred=np.array(new_valid_ypred)
    valid_mape = mape_loss(valid_y,new_valid_ypred)
    '''
    
    for i in range(len(valid_y)):
        print 'pred y & true y:' , valid_ypred[i] , valid_y[i]

    
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score in valid set is :' , valid_mape
    #预测
    result=gbrt.predict(test_X)
    joblib.dump(gbrt,'gbrt_up.model')
    
    submission = pd.DataFrame({'true gap': valid_y , 'pred_gap': valid_ypred}, columns=['true_gap','pred_gap'])
    submission.to_csv("/home/lael/Documents/gbrt_biggap.csv",index=False)
    
    return result 
    
def LRprediction(train_X,train_y,test_X):
    '''
        Ê¹ÓÃpipeline¹¹Ôì²ÎÊý·¶Î§£¬ÓÃGridSearchÑ¡È¡×îÓÅ²ÎÊý£¬Ò»°ãÀ´½²penaltyÓ¦¸ÃÊÇl2£¬loss_funcÎªlogº¯Êý£¬CÊÇl1µÄÈ¨ÖØÔ¼Êø¡£
    class_weightÉèÎª¡®auto¡¯µÄ»°£¬»ù±¾»áÊ¹×îºóµÄÑµÁ·½á¹ûÕý¸º±ÈÎª1£º10£¬¼ÙÈç'class_weight' :{ 0: 1.0 , 1 : 10.0}ÊÇÔÚ±íÃ÷
        ÑµÁ·Ñù±¾µÄÕý¸º±ÈÎª1:10£¬ËùÒÔ½«ÕýÑù±¾µÄÈ¨ÖØÉèÎª10£¬ÓÐÆ½ºâÑù±¾Õý¸º±ÈÀýµÄ¹¦ÄÜ¡£
    '''
    '''
    tuned_parameters =[{'penalty': ['l1'], 'tol': [1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000], 'class_weight':'auto'},\
                    {'penalty': ['l2'], 'tol':[1e-3, 1e-4],\
                     'C': [1, 10, 100, 1000],'class_weight':'auto'}]
    '''
    tuned_parameters =[{'penalty': ['l2'], 'tol': [1e-3, 1e-4],\
                     'C': [0.001, 0.01, 0.1, 1, 10, 100, 1000]}]
    #Çó×î¼Ñ²ÎÊý
    clf =GridSearchCV(LogisticRegression(), tuned_parameters, cv=5,scoring='mean_absolute_error')
    clf.fit(train_X,train_y)
    #»ñµÃ×î¼Ñf1·ÖÖµ
    print("    score: %0.3f" % clf.best_score_)
    #»ñµÃ×î¼Ñ²ÎÊý
    best_parameters = clf.best_estimator_.get_params()  
    for param_name in sorted(best_parameters.keys()):  
        print("\t%s: %r" % (param_name, best_parameters[param_name]))
    
    print("Generating LogisticRegressionClassifier model with parameters: ")
    #Ê¹ÓÃ×î¼Ñ²ÎÊý×öÑ±Á¼
    lgr = LogisticRegression(**best_parameters)
    
    
    #Ê¹ÓÃ×î¼Ñ²ÎÊý»­³ölearning curveÇúÏß£¬ÅÐ¶ÏÊÇ·ñ¹ýÄâºÏ
    #title = "RandomForestClassifier with hyperparams: "+str(best_parameters)
   # midpoint, diff = learningcurve.plot_learning_curve(lgr, title, train_X, train_y, (0.6, 1.01), cv=8, n_jobs=-1, plot=True)
  
    #a nparray result
    lgr.fit(train_X,train_y)
    print ( "score after best param ", lgr.score(train_X,train_y))
    #Ô¤²â
    result=lgr.predict(test_X)
    return result
    
def LRprediction2(train_X,train_y,test_X,valid_X, valid_y):
    tuned_parameters ={'penalty': 'l2', 'tol': 1e-4,\
                     'C': 1}
    lgr = LogisticRegression(**tuned_parameters)
    
    lgr.fit(train_X,train_y)
    result=lgr.predict(test_X)
    
    valid_ypred =  lgr.predict(valid_X)   
    valid_mape = mape_loss(valid_y,valid_ypred)
    
    print ' the mape score in valid set is :' , valid_mape
    return result   


if __name__ == '__main__':

    train=pd.read_csv('/home/lael/Documents/train_binary_downupSample11_2.csv',header=0)
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

    #ÓÃÂß¼­»Ø¹é×öÑµÁ·²¢Ô¤²â
    result = gbrtprediction2(train_X,train_y,test_X,valid_X, valid_y)
    
    #result = LRprediction2(train_X,train_y,test_X,valid_X, valid_y)
 
    print(type(result))
    #print(result)
    print 'step5:' , datetime.now()


    '''
    submission = pd.DataFrame({'pos':test.pos,'time':test.timeFrag,'gap':result}, columns=['pos','time','gap'])
    submission.to_csv("D:\\di_tech\\dataset\\season_1\\result\\submission_gbrt_biggap.csv",index=False)
    '''
