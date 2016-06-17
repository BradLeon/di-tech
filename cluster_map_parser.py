# -*- coding: utf-8 -*-
"""
Created on Thu May 26 13:45:12 2016
该文件用来得到训练集中的cluster_map中的区域Hash值与区域值相对应的字典
@author: Leon
"""

import os

FILE_NAME = os.getcwd() + '\\season_2\\training_data\\cluster_map\\cluster_map'

class cluster_map():
    
    def __init__(self):
        self.file_name = FILE_NAME
        self.pos_map = {}
        
    def get_pos_map(self):
        fp = open(self.file_name)
        for line in fp.readlines():
            pos_hash = line.strip('\n').split('\t')[0]
            pos_field = line.strip('\n').split('\t')[-1]
            
            self.pos_map[pos_hash] = pos_field
            
        return self.pos_map
            
if __name__=="__main__":
    my_cluster_map =  cluster_map()
    pos_map = my_cluster_map.get_pos_map()
    print pos_map
    
    