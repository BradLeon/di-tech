# -*- coding: utf-8 -*-
from __future__ import division  
import os
from GetPath import GetFilePath

#获取天气信息
class WeatherFeaturing():
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.weatherPath = cwd + '\\season_2\\training_data\\weather_data'
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
                self.weatherDic[timeFrag].extend([int(weatherLevel),float(temperature),int(pm25)])
            weatherFile.close()
                
        return self.weatherDic,weatherTimeFields

if __name__ == '__main__':
    WF = WeatherFeaturing()
    weatherDic,weatherTimeFields = WF.readWeatherData()
    print weatherDic