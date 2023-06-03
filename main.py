from toolsClass import OperationDataBase, DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate, DataModel
import os


if __name__ == '__main__':
    try:
        folderFiles = os.path.join(
            'home', 'fernando', 'Estacao'
        )
        dB = OperationDataBase('dado_diario')
        dB.setBd(1)
        dB2 = OperationDataBase('dado_diario')
        dB2.setBd(2)
        cM = ConverterMonths()
        fR = FileRetriever(f'/{folderFiles}')
        dD = DailyDate()
        dE = DataExtractor()
        dP = DataProcessor()
        dM = DataModel(dB)
        dM2 = DataModel(dB2)

        day = dD.extractDay(dD.yesterdayDate())
        year = dD.extractYear(dD.yesterdayDate())
        month = cM.getMonths(str(dD.extractMonth(dD.yesterdayDate())))
        fR.findYesterdayFile(month, year)
        for file in fR.getFoundFiles():
            data: list = dE.extractedDailyData(file, int(day))
            result = dE.getExtractData()
            dP.processedData(result)
            dM.executeDB(dP.getDataProcessed())
            dM2.executeDB(dP.getDataProcessed())

        dB.closeConnection()
        dB2.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
