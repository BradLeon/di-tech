# -*- coding: utf-8 -*-
'''
该文件用来将得到的结果进行取整处理
by: Lael  2016-6-4 11:16
'''



import csv
import pandas as pd

inFile = file('submission_gbrt_6.csv','rb')
reader = csv.reader(inFile)
resultList = []
count = 0
for line in reader:
    if count == 0:
        count += 1
        #resultList.append(line)
        continue
    else:
        pos,time,gap = line
        if float(gap) < 1:
            resultList.append([pos,time,1.0])
        else:
            gap = round(float(gap)) 
            #print type(gap),gap,'----'
            #print gap
            resultList.append([pos,time,gap])
            #print type(gap),'  gap:',gap
    count += 1
inFile.close()  

  
''' 不管怎么改，写入的浮点数都会自动转换为整形'''
outFile = file('submissionProcessed.csv','wb')
writer = csv.writer(outFile)
writer.writerows(resultList)
outFile.close()

'''
用来测试生成的文件的每一行数据的格式
发现以float格式写入文件中，读取出来还是str
并且都自动省略了.0
iifile = file('submissionProcessed.csv','rb')
reader = csv.reader(iifile)
for line in reader:
    pos,time,gap = line
    print type(pos),type(time),type(gap),pos,time,gap
iifile.close()
'''

'''
for line in resultList:
    writer.writerow(line)
    print line
'''

'''
df = pd.DataFrame(resultList)
df.to_csv('submissionProcessed.csv')
'''