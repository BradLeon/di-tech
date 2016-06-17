# -*- coding: utf-8 -*-

'''
下面的程序用来测试POI文件里面的信息
'''    
import os
from cluster_map_parser import cluster_map

class GetPoiDic():
    def getPoiDic(self):
        clusterMap = cluster_map()
        dist2intDic = clusterMap.get_pos_map() 
        path = os.getcwd() + r'\season_2\training_data\poi_data\poi_data'
        poiList = ['24#1','19','20#8','19#3','13#4','20','11#8','11#4']
        '''poiDic.keys() ----> 1-66
        {'30': ['4#16:166', '25:498', '20:1162', '22:249', ...], '42': [], ...}
        '''
        poiDic = {}
        for line in open(path):
            tempLi = []
            for item in line.strip('\n').split('\t'):
                tempLi.append(item)
            distNum = dist2intDic[tempLi[0]]
            poiDic[distNum] = []
            for i in tempLi[1:]:
                poiDic[distNum].append(i) 
        
        '''districtPerNums: {1:{1:2256,2:345,3:2334,...25:9872}, 2:{},..., 66:{}}
           districtTotalNums 用来统计各大区域中的总设施个数
        '''
        districtPerNums = {}
        districtTotalNums = {}
        
        for i in range(1,len(dist2intDic)+1):
            districtPerNums[i] = {}
            districtTotalNums[i] = 0
            
        for key,value in poiDic.items():
            for item in value:
                '''得到大类标签 如1#22:398 中的1#22'''
                classLabel = item.split(':')[0]
                classNum = int(item.split(':')[1])
                key = int(key)
                if classLabel in poiList:
                    try:
                        districtPerNums[key][classLabel] += classNum
                    except:
                        #print key,classLabel,classNum            
                        districtPerNums[key][classLabel] = classNum
                    
        '''判断districtPerNums中各个大区域是不是有25个类别，如果没有输出该区域，并将该设施数目补充为0'''
        facilityNum = 6
        dPerNums = districtPerNums
        for key,value in districtPerNums.items():
            for classLabel in poiList:
                if classLabel not in value.keys():
                    dPerNums[key][classLabel] = 0
        
        ''' 用来做简单的测试 看是否每个区域中的设施类别数目达到了25个          
        cc = 0
        for key,value in dPerNums.items():
             if value.keys < 25:
                 print key
                 cc += 1
        print '还有缺失的个数为：',cc
        '''   
        
        districtPerNums = dPerNums
        

        '''  2016-06-15 21:15       
        #统计每个大区域1-66 中的设施总数目
        for key,value in districtPerNums.items():
            vcount = 0
            for k,v in value.items():
                vcount += v
            districtTotalNums[key] = vcount
        #print districtTotalNums
        
        #将各个区域的数据组合： {1:[1设施数目,2设施数目,3,4,...25,本区域的总数]}
        disFacilityNumDic = {}
        for i in range(1,67):
            disFacilityNumDic[i] = []
        for key,value in districtPerNums.items():
            for k,v in value.items():
                disFacilityNumDic[key].append(v)
            #添加该区域的总设施数目
            disFacilityNumDic[key].append(districtTotalNums[key])
        '''
        return districtPerNums


if __name__ == '__main__':
    GPC = GetPoiDic()
    
    districtPerNums = GPC.getPoiDic()
    print districtPerNums


























