# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 10:59:23 2019

@author: Arka Bhattacharya
"""

from ExtractionTestAsynch import *
from DataClasses import*
from multiprocessing import Pool

#main script to run data collection of each firm
downloadIndexFiles('./indexFiles/', 2016, 2017)
firmGroup = CompanyExtraction()
years = [2016,2017]
firmGroup.companyList = firmGroup.companyList[0:100]
if __name__ == '__main__':
    for year in years:
        #print("Finding and extracting data for " + str(firm.companyName) + "...")
        for qtr in range(1,5):
            path = "./indexFiles/master_index_" + str(year) + "_QTR" + str(qtr) + ".idx"
            for firm in firmGroup.companyList: 
                print("Adding data for " + firm.companyName)              
                firmData = findData(firm, path)
                urlSet = [url[1] for url in firmData]
                #initiate asynchronous function.
                loop = asyncio.get_event_loop()
                data = loop.run_until_complete(getAllHtmls(urlSet, loop))
                firm.addData(extract(data, firmData))
    loop.close()
firmGroup.writeData() 
print("Finished")           
#file.close()