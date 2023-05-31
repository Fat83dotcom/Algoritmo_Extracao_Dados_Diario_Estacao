from toolsClass import OperationDataBase, DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate, DataModel


if __name__ == '__main__':
    try:
        dB = OperationDataBase('dado_diario')
        cM = ConverterMonths()
        fR = FileRetriever('.')
        dD = DailyDate()
        dE = DataExtractor()
        dP = DataProcessor()
        dM = DataModel(dB)

        day = dD.extractDay(dD.yesterdayDate())
        year = dD.extractYear(dD.yesterdayDate())
        month = cM.getMonths(str(dD.extractMonth(dD.yesterdayDate())))
        fR.findYesterdayFile(month, year)
        print(list(fR.getFoundFiles()))
        for file in fR.getFoundFiles():
            data: list = dE.extractedDailyData(file, int(day))
            result = dE.getExtractData()
            dP.processedData(result)
            dM.executeDB(dP.getDataProcessed())

        dB.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
