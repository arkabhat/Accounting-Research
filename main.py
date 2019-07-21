 # -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 10:59:23 2019

@author: Arka Bhattacharya
"""

from ExtractionModule import *
from DataClasses import*
from multiprocessing import Pool
import time

start = time.time()
#main script to run data collection of each firm
#downloadIndexFiles('./indexFiles/', 2018, 2018)
startFirm = input("Enter start firm no: ")
endFirm = input("Enter end firm no: ")

firmGroupings = range(int(startFirm), int(endFirm) + 1, 200)
loop = None

def addData(firm, path, loop): 
    #print("Adding data for " + firm.companyName)              
    firmData = findData(firm, path)
    urlSet = [url[1] for url in firmData]
    #initiate asynchronous function.
    
    data = loop.run_until_complete(getAllHtmls(urlSet, loop))
    firm.addData(extract(data,firmData))

def main(startNum, endNum, loop):
    years = range(2005, 2019)
    total = len(years) * 5
    startYear = years[0]
    firmGroup = CompanyExtraction()
    firmGroup.companyList = firmGroup.companyList[startNum:endNum]
    
    for i in range(1, total):
        if i%5 == 0:
            startYear = startYear + 1
            i = i + 1
        qtr = i%5
        print("Finding and extracting data for year " + str(startYear) + ". . .")
        print("Finding and extracting data for qtr " + str(qtr) + ". . .")
        path = "./indexFiles/master_index_" + str(startYear) + "_QTR" + str(qtr) + ".idx"
        for firm in firmGroup.companyList:
            addData(firm, path, loop)   
    
    firmGroup.writeData(startNum, endNum)
    print("Finished for Group: " + str(startNum) + " to " + str(endNum))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for x in range(len(firmGroupings) - 1):
        print("Adding for group " + str(firmGroupings[x]) + " to " + str((firmGroupings[x+1] - 1)))
        main(firmGroupings[x], firmGroupings[x+1], loop)
    loop.close()


print("Finished time: " + str((time.time() - start)))           
#file.close()