from toolsClass import DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate
from DataBaseManager.LogFiles import LogTimeMixin
from DataBaseManager.databaseSettings import dbCredentials
from DataBaseManager.OperationalDataBase import DataModel
from DataBaseManager.OperationalDataBase import OperationDataBase
import os


class MainWorker:
    pass


if __name__ == '__main__':
    log = LogTimeMixin()
    init = log.snapshotTime()
    try:
        log.registerTimeLogStart()
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
            dM.executeInsertDadoDiarioTable(dP.getDataProcessed())
            dM2.executeInsertDadoDiarioTable(dP.getDataProcessed())
            dM3.executeInsertDadoDiarioTable(dP.getDataProcessed())
    except Exception as e:
        print(e)
    finally:
        log.registerTimeLogEnd()
        end = log.snapshotTime()
        log.registerTimeElapsed(init, end)
