# -*- coding: utf-8 -*-
import os

class GetFilePath():
    '''
    获取某一个文件夹下面的所有文件的Path,存储为list,用于后面的读取数据    
    '''
    def getFilePath(self,path):
        infoList = []
        #从路径  C:\pyWorkspace\DIDI\training_data 下读取文件，该路径和当前目录cwd有关
        for root,dirs,files in os.walk(path):
            if len(files) != 0:
                for order in files:
                    infoList.append(path + '\\' + order)
        '''
        #读取拥堵信息root: C:\pyWorkspace\DIDI\test_set_1\traffic_data
        #dirs:[]
        #files:[各个文件名]
        for root,dirs,files in os.walk(path):
            for traffic in files:
                trafficList.append(path + '\\' + traffic)
        '''       
        return infoList