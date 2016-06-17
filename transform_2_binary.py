# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 13:45:30 2016

@author: Leon
"""
import pandas as pd

def transform_dataframe_binaryfeature(df):
      df2 = df.copy()

      series1 = df['w1_pm2.5'] < 130
      series2 = df['w1_pm2.5'] > 200
      
      df2['w1_pm2.5'] = series1+series2
      
      df2['facilityNum1'] = df['facilityNum1']<5500
      df2['facilityNum2'] = df['facilityNum2']<5000

      df2['facilityNum3'] = df['facilityNum3']<1000
      df2['facilityNum4'] = df['facilityNum4']<14000
      df2['facilityNum5'] = df['facilityNum5']<3000
      df2['facilityNum6'] = df['facilityNum6']<6500
      df2['facilityNum7'] = df['facilityNum7']<5500
      df2['facilityNum8'] = df['facilityNum8']<5000
      df2['facilityNum9'] = df['facilityNum9']==14
      df2['facilityNum10'] = df['facilityNum10']==25
      df2['facilityNum11'] = df['facilityNum11']<30000
      
      series1 = df['facilityNum14'] < 8000
      series2 = df['facilityNum14'] > 30000
      
      df2['facilityNum14'] = series1+series2
      df2['facilityNum15'] = df['facilityNum15']<6000
      df2['facilityNum16'] = df['facilityNum16']<16000
      df2['facilityNum17'] = df['facilityNum17']<6000
      df2['facilityNum19'] = df['facilityNum19']<30000
      df2['facilityNum20'] = df['facilityNum20']<30000
      df2['facilityNum21'] = df['facilityNum21']>600
      df2['facilityNum22'] = df['facilityNum22']<5000
      df2['facilityNum23'] = df['facilityNum23']<5000
      df2['facilityNum24'] = df['facilityNum24']<20000
      df2['facilityNum25'] = df['facilityNum25']<6000
      df2['facilityTotalNum'] = df['facilityTotalNum']<30000

      
      for i in ['1','2','3']:
          
          df2['meanPrice'+i] = df['meanPrice'+i]>30
          
      return df2
    
    
if __name__ =='__main__':
    df = pd.read_csv('/home/lael/Documents/trainResult0611_downupSample.csv',header=0)
    df2= transform_dataframe_binaryfeature(df)
    df2.to_csv('/home/lael/Documents/trainResult0611_downupSample_binary.csv',index=False)
    
    