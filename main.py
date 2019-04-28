# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 10:59:23 2019

@author: Arka Bhattacharya
"""

from ExtractionTestAsynch import *
from DataClasses import*
from multiprocessing import Pool
import time

start = time.time()
#main script to run data collection of each firm
#downloadIndexFiles('./indexFiles/', 2018, 2018)
startFirm = input("Enter start firm no: ")
endFirm = input("Enter end firm no: ")

firmGroupings = range(int(startFirm), int(endFirm) + 1, 200)
years = [2017, 2018]
total = len(years) * 4
startYear = years[0]

def addData(firm, path, loop): 
    #print("Adding data for " + firm.companyName)              
    firmData = findData(firm, path)
    urlSet = [url[1] for url in firmData]
    #initiate asynchronous function.
    
    data = loop.run_until_complete(getAllHtmls(urlSet, loop))
    firm.addData(extract(data,firmData))

def main(startNum, endNum):
    if __name__ == '__main__':
        firmGroup = CompanyExtraction()
        firmGroup.companyList = firmGroup.companyList[startNum:endNum]
        loop = asyncio.get_event_loop()
        for i in range(1, total  + 1):
            if i%4 == 0:
                qtr = 4
            else:
                qtr = i%4
            #print("Finding and extracting data for " + str(firm.companyName) + "...")
            path = "./indexFiles/master_index_" + str(2016 + int((i/4))) + "_QTR" + str(qtr) + ".idx"
            for firm in firmGroup.companyList:
                addData(firm, path, loop)   
        loop.close()
    firmGroup.writeData(startNum, endNum) 

for x in range(len(firmGroupings)):
    print("Adding for group" + firmGroupings[x] + "to" + (firmGroupings[x+1] - 1))
    main(firmGroupings[x], firmGroupings[x+1])




print("Finished time: " + str((time.time() - start)))           
#file.close()