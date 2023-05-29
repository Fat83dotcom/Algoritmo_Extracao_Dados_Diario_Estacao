from toolsClass import OperationDataBase, DataExtractor, FileRetriever
from datetime import datetime


class DailyDate:
    def __init__(self, ) -> None:
        self.__dailyDate: list = []
        self.__todayDate = datetime.now()

    def extractedDailyData(self, pathFile: str, dateTarget: int) -> None:
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


if __name__ == '__main__':
    try:
        dB = OperationDataBase('dado_diario')
        r = FileRetriever('.')
        for file in r.getFoundFiles():
            print(file)

        print('hello')
        f = date
        dB.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
