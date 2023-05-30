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

    def yesterdayDate(self):
        return self.__todayDate - timedelta(1)

    def getTodayDate(self) -> datetime:
        return self.__todayDate


if __name__ == '__main__':
    try:
        # dB = OperationDataBase('dado_diario')
        r = FileRetriever('.')
        d = DailyDate()
        fileTarget: str = r.generatorPathTargetFile('abr', '2022')
        print(d.getTodayDate())
        print(d.yesterdayDate())
        # data = d.extractedDailyData(fileTarget, 30)
        # print(data)

        # dB.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
