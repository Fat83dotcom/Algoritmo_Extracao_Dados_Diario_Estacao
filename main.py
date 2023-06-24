from toolsClass import OperationDataBase, DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate, DataModel
from databaseSettings import dbCredentials
import sys 
sys.setrecursionlimit(10**6)
import os
 

if __name__ == '__main__':
    try:
        dBTable = 'dado_diario'
        folderFiles = os.path.join(
            'home', 'fernando', 'Estacao'
        )
        dB = OperationDataBase(dBTable, dbCredentials(1))
        dB2 = OperationDataBase(dBTable, dbCredentials(2))
        dB3 = OperationDataBase(dBTable, dbCredentials(3))
        cM = ConverterMonths()
        fR = FileRetriever(f'/{folderFiles}')
        dD = DailyDate()
        dE = DataExtractor()
        dP = DataProcessor()
        dM = DataModel(dB)
        dM2 = DataModel(dB2)
        dM3 = DataModel(dB3)

        day = dD.extractDay(dD.yesterdayDate())
        year = dD.extractYear(dD.yesterdayDate())
        month = cM.getMonths(str(dD.extractMonth(dD.yesterdayDate())))
        fR.findYesterdayFile(month, year)
        for file in fR.getFoundFiles():
            dE.extractedDailyData(file, int(day))
            result = dE.getExtractData()
            dP.processedData(result)
            dM.executeDB(dP.getDataProcessed())
            dM2.executeDB(dP.getDataProcessed())
            dM3.executeDB(dP.getDataProcessed())

        dB.closeConnection()
        dB2.closeConnection()
        dB3.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
