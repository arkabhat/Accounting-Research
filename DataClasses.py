#Represents a Company with a name, CIK, and data
class Company:
    def __init__(self, compName, compCIK):
        self.companyName = compName
        self.cik = compCIK
        self.data = []

    def addData(self, data):
        self.data += data

#Used mainly for testing mechanisms, reperesents the data in association with
#the company.
class Data:
    def __init__(self,compName, cik, urlData):
        self.companyName = compName
        self.cik = cik
        self.urlData = urlData
        

    
