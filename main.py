from toolsClass import DataProcessor, FileRetriever
from toolsClass import DataExtractor, ConverterMonths, DailyDate
from DataBaseManager.LogFiles import LogTimeMixin, LogErrorsMixin
from DataBaseManager.databaseSettings import dbCredentials
from DataBaseManager.OperationalDataBase import DataModel
from DataBaseManager.OperationalDataBase import OperationDataBase
import os


class MainWorker(LogTimeMixin, LogErrorsMixin):
    def __init__(self) -> None:
        try:
            self.folderFiles = os.path.join(
                'home', 'fernando', 'Estacao'
            )
            self.dB = OperationDataBase(dbCredentials(1))
            self.dB2 = OperationDataBase(dbCredentials(2))
            self.dB3 = OperationDataBase(dbCredentials(3))
            self.cM = ConverterMonths()
            self.fR = FileRetriever(f'/{self.folderFiles}')
            self.dD = DailyDate()
            self.dE = DataExtractor()
            self.dP = DataProcessor()
            self.dM = DataModel(self.dB)
            self.dM2 = DataModel(self.dB2)
            self.dM3 = DataModel(self.dB3)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__init__.__name__
            self.registerErrors(className, methName, e)

    def run(self):
        init = self.snapshotTime()
        try:
            self.registerTimeLogStart()
            dDiario = 'dado_diario'
            day = self.dD.extractDay(self.dD.yesterdayDate())
            year = self.dD.extractYear(self.dD.yesterdayDate())
            month = self.cM.getMonths(str(self.dD.extractMonth(self.dD.yesterdayDate())))
            self.fR.findYesterdayFile(month, year)
            for file in self.fR.getFoundFiles():
                self.dE.extractedDailyData(file, int(day))
                result = self.dE.getExtractData()
                self.dP.processedData(result)
                self.dM.execInsertDDiario(dDiario, self.dP.getDataProcessed())
                self.dM2.execInsertDDiario(dDiario, self.dP.getDataProcessed())
                self.dM3.execInsertDDiario(dDiario, self.dP.getDataProcessed())
        except Exception as e:
            print(e)
        finally:
            self.registerTimeLogEnd()
            end = self.snapshotTime()
            self.registerTimeElapsed(init, end)


if __name__ == '__main__':
    _exec = MainWorker()
    _exec.run()
