# -*- coding: utf-8 -*-
import os
from GetPath import GetFilePath
from cluster_map_parser import cluster_map

class TrafficFeaturing():
    def __init__(self):
        #得到当前的目录
        cwd = os.getcwd()
        self.trafficPath = cwd + '\\season_2\\training_data\\traffic_data'
        self.trafficDic = {}
        #得到训练集中的所有文件名字列表
        GFP = GetFilePath()
        self.trafficList = GFP.getFilePath(self.trafficPath)        
        #读入区域哈希值与整型值的对应信息
        clusterMap = cluster_map()
        self.dist_hash2intDic = clusterMap.get_pos_map() 
        
        for pos_field in range(1,67):
            pos_field = str(pos_field)
            self.trafficDic[pos_field] = {}
            
            '''
            for time_field in self.demand_time_fields:
                predictFrag = predictDate + '-' + str(time_field)
                self.trafficDic[pos_field][predictFrag] = 0
            '''
                
    def readTrafficData(self):
        #一个一个traffic文件打开        
        for traffic in self.trafficList:
            trafficFile = open(traffic)
            for line in trafficFile:
                district_hash,oneLevel,twoLevel,threeLevel,fourLevel,timePoint = line.split('\t')
                timePoint = timePoint.split('\n')[0]
                #Date:2016-01-01
                Date = timePoint.split()[0]
                #Time:11:26:25
                Time = timePoint.split()[1]                
                timeInfo = Time.split(':')
                
                hour = int(timeInfo[0])
                minute = int(timeInfo[1])
                time_field = hour*6 + minute/10 + 1
                timeFrag = Date + '-' + str(time_field)    
                pos_field = self.dist_hash2intDic[district_hash]
                self.trafficDic[pos_field][timeFrag] = []
                self.trafficDic[pos_field][timeFrag].extend([int(oneLevel.split(':')[1]), int(twoLevel.split(':')[1]), int(threeLevel.split(':')[1]), int(fourLevel.split(':')[1])])
            trafficFile.close()
        '''print '----:',self.trafficDic['2']['2016-01-30-103']'''     
        return self.trafficDic