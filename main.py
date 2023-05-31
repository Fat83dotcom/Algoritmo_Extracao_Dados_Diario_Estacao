from toolsClass import OperationDataBase, DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate, DataModel


if __name__ == '__main__':
    try:
        dB = OperationDataBase('dado_diario')
        m = ConverterMonths()
        r = FileRetriever('.')
        d = DailyDate()
        eX = DataExtractor()
        eP = DataProcessor()
        mD = DataModel(dB)

        day = d.extractDay(d.yesterdayDate())
        year = d.extractYear(d.yesterdayDate())
        month = m.getMonths(str(d.extractMonth(d.yesterdayDate())))
        r.findYesterdayFile(month, year)
        print(list(r.getFoundFiles()))
        for file in r.getFoundFiles():
            data: list = eX.extractedDailyData(file, int(day))
            result = eX.getExtractData()
            eP.processedData(result)
            mD.executeDB(eP.getDataProcessed())

        dB.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
