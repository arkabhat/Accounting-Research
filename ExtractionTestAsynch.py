# -*- coding: utf-8 -*-
"""
Created on Sat Mar 30 16:37:15 2019

@author: Arka Bhattacharya
"""

from DataClasses import*
import pandas as pd
import numpy as np
import urllib.request as ulib
from bs4 import BeautifulSoup as soup
import lxml
import re
import aiohttp
import asyncio
import requests

#downloads master index files
def downloadIndexFiles(folderPath, fromYear, toYear):
        base_link = "https://www.sec.gov/Archives/edgar/full-index"

        for year in range(fromYear, toYear + 1):
            for qtr in range(1, 5):
                # Get the URL and its text for each quarter/year
                web_link = base_link + "/" + str(year) + "/QTR" + str(qtr) + "/master.idx"
                text = requests.get(web_link).text
                # Write the text to the file
                local_path = "master_index_" + str(year) + "_QTR" + str(qtr) + ".idx"
                file = open(folderPath + local_path, "w")
                file.write(text)
                file.close()
#Class Representing a group of companies and their data
class CompanyExtraction:         
        def __init__(self):
            self.companyList = self.getAllCompanies()

        #gets all companies from provided firmlist and creates
        # a list of company objects for each company in the firmlist file.        
        def getAllCompanies (self):
            print("Grabbing all companies from file")
            file = "./firmlist_20190125.xlsx"
            dataFile = pd.ExcelFile(file)
            infoFrame = dataFile.parse("Data")
            companyList = []
            firmName = infoFrame['conm']
            firmCik = infoFrame['cik']
            for n in range(firmName.size - 1):
                newFirm = Company(firmName[n], firmCik[n])
                companyList.append(newFirm)
            return companyList
        
        #writes the data of each company to a .csv file
        def writeData(self):
            print("Writing data to .csv file")
            fileName = "./firmData.csv"
            dataArray = []
            for firm in self.companyList:
                for data in firm.data:
                    if data and data[2] :
                        dataArray.append([firm.companyName, firm.cik, data[0], data[1], data[2]])
                print("Wrote data for " + firm.companyName)
            if dataArray:
                dataArray = np.array(dataArray)
                dataFrame = pd.DataFrame(dataArray, columns = ["Firm Name", "Firm CIK","Date Submitted", "URL", "Firm Data"])
                dataFrame.to_csv(fileName, index = 0)
            else:
                dataFrame = pd.DataFrame()
                dataFrame.to_csv(fileName, index = 0 )

#given a url, gets the html text of the url
async def getHTML(session, url):
    #with aiohttp.Timeout(10):
    root = "https://www.sec.gov/Archives/"
    async with session.get(root + url) as response:
        urlVal = await response.text(encoding = 'utf-8')
    return urlVal
 
#uses getHTML method on a group of urls asynchronously
async def getAllHtmls(urls, loop):
    async with aiohttp.ClientSession(loop = loop) as session:
        results = await asyncio.gather(*[loop.create_task(getHTML(session, url))for url in urls])
    return results

#extracts data for each firm and a respective 8-K file of 
#said firm.
def extract(data, firmData):
    #final list of data to be returned is initialized
    totalList = [] 
    for x in range(len(data)):
        #for each html filing, reduce the amount of html
        #to be parsed to be between <DOCUMENT> and the 
        #signatures section
        entry = data[x].splitlines()
        endIndex = 0
        for line in entry:
            if "SIGNATURE" in line:
                endIndex = entry.index(line)
        entry = entry[entry.index("<DOCUMENT>") : endIndex + 1] 
        
        #convert the trimmed html into a string form
        dataStrng = '\n'.join(str(line) for line in entry)

        #parse the trimmed html and find all cases of the following pattern : "Item X.XX"
        newData = soup(dataStrng, 'lxml')
        filing = newData.find_all(string = re.compile("^Item [0-9]\.[0-9][0-9]$"))
        blurb = ""
        for entry in filing:
            #If desired item is one of the found pattern matches
            if "Item 5.02" in entry:
                difference = []
                targetInfo = entry.find_all_next()
                if(len(filing) == 1):
                    for val in targetInfo:
                        string = str(val.string)
                        if(targetInfo.index(val) == 0 or string not in str(targetInfo[targetInfo.index(val)-1].string)):
                            difference.append(string)
                else:
                    nextInfo = filing[filing.index(entry) + 1].find_all_next()
                    for val in targetInfo:
                        if val not in nextInfo:
                            string = str(val.string)
                            if(targetInfo.index(val) == 0 or string not in str(targetInfo[targetInfo.index(val)-1].string)):
                                difference.append(string)
                blurb += " ".join(str(line) for line in difference)
            totalList.append([firmData[x][0], firmData[x][1], blurb])    
    return totalList

#Finds all the potential urls for filings that are present.
def findData(company, path):
    #run through index files and find all files with that company
    #parse through them and figure out which ones matter, add to a list
    #return that list of urls
    entries = []
    file = open(path)
    for line in file:
        if "|" in line:
            indexEntry = line.split("|")
            if str(company.cik) in indexEntry and "8-K" in indexEntry:                         
                    entries.append([indexEntry[3],indexEntry[4]])
                        

    return entries


