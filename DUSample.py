#coding=utf-8
import os
import numpy as np
import scipy
import pandas as pd
import csv
import random
import SMOTE
'''
使用上下抽样方法构造正负类均衡的训练样本，最后得到 负样本：正样本=10:1，这是训练集的最佳比例
可以使用这部分函数重新处理训练集 再将训练集用于逻辑回归训练
'''
def shuffle(df, n=1, axis=0):     
    df = df.copy()
    for _ in range(n):
        df.apply(np.random.shuffle, axis=axis)
    return df

#对buy=0的数据下抽样，取30倍正样本数目大小
def downSample(df):
    indices = np.where(df.buy == 0)[0]
    rng = np.random.RandomState(13)
    rng.shuffle(indices)
    n_pos = (df.buy == 1).sum()*20
    df = df.drop(df.index[indices[n_pos:]])
    return df
#对buy=1的数据上抽样，取3倍的正样本数目大小
def upSample(df):
    df.drop(['pos','timeFrag'],axis=1,inplace=True)
    posit=df[df.predictGap>10]
    Synthetic=SMOTE.SMOTE(posit.values,400,5)
    oColumns=df.columns.values
    newSample=pd.DataFrame(Synthetic,columns=oColumns)

    #rows = random.sample(list(df.index), len(df))
    #df=df.ix[rows]
    df=pd.concat([df,newSample],axis=0)
    df=df.reset_index()
    df.reindex(np.random.permutation(df.index))
    return df
    
    
        

#测试
#df = pd.read_csv('train.csv', header=0)
df=pd.read_csv('/home/lael/Documents/trainResult0611.csv',header=0)
print len(df)
# downsample negative cases -- there are many more negatives than positives
#df = downSample(df)
#print(len(df))
df = upSample(df)
print len(df)

df.to_csv("/home/lael/Documents/trainResult0611_downupSample.csv",index=True)
#predictions_file = open("D:\\di_tech\\dataset\\season_1\\train_downupSample.csv", "w")
#open_file_object = csv.writer(predictions_file)
#open_file_object.writerow(df.columns.values)
#open_file_object.writerows(df.values)
#predictions_file.close()
