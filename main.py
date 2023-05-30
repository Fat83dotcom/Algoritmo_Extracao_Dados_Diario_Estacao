from toolsClass import OperationDataBase, DataProcessor, FileRetriever
from datetime import datetime, timedelta


class DailyDate:
    '''Extrai os dados do dia anterior'''
    def __init__(self) -> None:
        self.__dailyDate: list = []
        self.__todayDate: datetime = datetime.now()

    def extractedDailyData(self, pathFile: str, dateTarget: int) -> None:
        '''Informe o caminho do arquivo e a data da extração. Os dados
        são salvos no atributo __dailyDate: list'''
        with open(pathFile, 'r', encoding='utf-8') as file:
            data = file.readlines()
            extractedDataTarget: list = []
            count = -1
            while True:
                datas = data[count].strip()[:3]
                if int(datas) < dateTarget:
                    break
                else:
                    extractedDataTarget.append(data[count].strip().split(','))
                    count -= 1
        return extractedDataTarget  # type: ignore

    def yesterdayDate(self) -> datetime:
        '''Retorna a data de ontem.'''
        return self.__todayDate - timedelta(1)

    def getTodayDate(self) -> datetime:
        '''Retorna o atributo __todayDate, contendo a data atual.'''
        return self.__todayDate

    def extractMonth(self, date: datetime) -> str:
        '''Retorna o mês da data informada.'''
        dm = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcMonth = dm.strftime('%m')
        return extratcMonth

    def extractYear(self, date: datetime) -> str:
        '''Retorna o ano da data informada.'''
        dt = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcYear = dt.strftime('%Y')
        return extratcYear


if __name__ == '__main__':
    try:
        # dB = OperationDataBase('dado_diario')
        r = FileRetriever('.')
        d = DailyDate()
        e = DataProcessor()
        fileTarget: str = r.generatorPathTargetFile('abr', '2022')
        print(d.getTodayDate())
        print(d.yesterdayDate())
        print(d.extractYear(d.getTodayDate()))
        print(d.extractMonth(d.getTodayDate()))

        data: list = d.extractedDailyData(fileTarget, 30)

        e.processedData(data)

        print(e.getDataProcessed())

        # for i in data:
        #     print(i)

        # dB.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
