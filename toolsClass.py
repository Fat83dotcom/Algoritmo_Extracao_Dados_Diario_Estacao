import os
import csv
from pathlib import Path
from itertools import groupby
from datetime import datetime, timedelta
from statistics import mean, median, mode
from DataBaseManager.LogFiles import LogErrorsMixin


class FileRetriever(LogErrorsMixin):
    '''
        Busca arquivos, manipula caminhos e nomes de arquivos.
    '''
    def __init__(self, pathTarget, extension='.csv') -> None:
        self.__foundFiles: list = []
        self.__pathTarget = pathTarget
        self.__extensionFile = extension

    def findYesterdayFile(self, month, year) -> None:
        '''
            Busca o arquivo cujo o mês está na data de ontem.
            Salva o arquivo no atributo self.__foundFiles
            Retorna -> None
        '''
        try:
            fileName = self.__generatorNameFile(month, year)
            self.__foundFiles.append(
                self.findOneFile(fileName)
            )
        except Exception as e:
            className = self.__class__.__name__
            methName = self.findYesterdayFile.__name__
            self.registerErrors(className, methName, e)

    def __findFiles(self) -> None:
        '''
            Atributo de classe.
            Busca todos os arquivos cujo a extensão foi definida na pasta.
            Salva o caminho dos arquivos no atributo self.__foundFiles.
            Retorna -> None.
        '''
        try:
            for root, _, file_ in os.walk(self.__pathTarget):
                for targetFile in file_:
                    if self.__extensionFile in targetFile:
                        self.__foundFiles.append(
                            os.path.join(root, targetFile)
                        )
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__findFiles.__name__
            self.registerErrors(className, methName, e)

    def findOneFile(self, fileName: str | None) -> str | None:
        '''
            Busca um arquivo na pasta definida pelo seu nome.
            Retorna o caminho do arquivo se ele existir.
        '''
        try:
            for root, _, file_ in os.walk(self.__pathTarget):
                for targetFile in file_:
                    if fileName in targetFile:
                        return str(os.path.join(root, targetFile))
            return 'Arquivo não encontrado.'
        except Exception as e:
            className = self.__class__.__name__
            methName = self.findOneFile.__name__
            self.registerErrors(className, methName, e)

    def __generatorNameFile(self, month, year) -> str | None:
        '''
            Atributo de classe.
            Gera o nome de um arquivo baseado em seu mês e ano.
            Retorna o nome do arquivo.
        '''
        try:
            nameFile = os.path.join(
                f'{month}_{year}_log{self.__extensionFile}'
            )
            return nameFile
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__generatorNameFile.__name__
            self.registerErrors(className, methName, e)

    def getFoundFiles(self):
        '''
            Retorna o atributo self.__foundFiles.
        '''
        try:
            if self.__foundFiles:
                for files in self.__foundFiles:
                    yield files
            else:
                raise Exception('Arquivos não encontrdos')
        except Exception as e:
            className = self.__class__.__name__
            methName = self.getFoundFiles.__name__
            self.registerErrors(className, methName, e)


class DataExtractor(LogErrorsMixin):
    '''
        Extrai os dados brutos dos arquivos e agrupa-os por dia.
    '''
    def __init__(self) -> None:
        self.__extractData: list = []

    def dataExtract(self, file: list) -> None:
        '''
            Extrai dados de arquivos .csv.
            Paramtros:
            file -> Nome do arquivo.
            O dados são salvos no atributo self.__extractData
            pelo metodo de classe self.__groupbyDataByDate
        '''
        try:
            PATH_CSV = Path(__file__).parent / file  # type: ignore
            with open(PATH_CSV, 'r', encoding='utf-8') as myCsv:
                reader = csv.reader((line.replace('\0', '') for line in myCsv))
                self.__groupbyDataByDate(reader)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.dataExtract.__name__
            self.registerErrors(className, methName, e)

    def extractedDailyData(self, pathFile: str, dateTarget: int):
        '''Informe o caminho do arquivo e a data da extração. Retorna os dados
        retirados do arquivo'''
        try:
            with open(pathFile, 'r', encoding='utf-8') as file:
                dataFile = [x.replace('\0', '') for x in file.readlines()]
                extractDataTarget: list = []
                for data in dataFile[-1::-1]:
                    datas = data[:3].strip()
                    if datas == '':
                        continue
                    if int(datas) > dateTarget:
                        ...
                    elif int(datas) == dateTarget:
                        extractDataTarget.append(
                            data.strip().split(',')
                        )
                    else:
                        break
            self.__groupbyDataByDate(extractDataTarget)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.extractedDailyData.__name__
            self.registerErrors(className, methName, e)

    def __groupbyDataByDate(self, iterable):
        '''
            Agrupa os dados por data.
            Salva os dados no atributo self.__stractData
        '''
        def __extractKey(listTarget):
            try:
                return listTarget[0][:11]
            except Exception as e:
                className = self.__class__.__name__
                methName = __extractKey.__name__
                self.registerErrors(className, methName, e)
        try:
            groups = groupby(iterable, key=__extractKey)
            for date, data in groups:
                self.__extractData.append((date, [
                    (
                        float(value[1]),
                        float(value[2]),
                        float(value[3]),
                        float(value[4])
                    )
                    if
                    value[1] and value[2] and value[3] and value[4] != ''
                    else (0, 0, 0, 0)
                    for value in data
                ]))
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__groupbyDataByDate.__name__
            self.registerErrors(className, methName, e)

    def getExtractData(self) -> list:
        '''
        Retorna o atributo self.__extractData
        '''
        return self.__extractData


class DataProcessor(LogErrorsMixin):
    '''Processa os dados e prapara-os para entrar no banco de dados.'''
    def __init__(self) -> None:
        self.__dataProcessed: list = []
        self.__numbersOfMonth = {
            'jan': 1,
            'fev': 2,
            'mar': 3,
            'abr': 4,
            'mai': 5,
            'jun': 6,
            'jul': 7,
            'ago': 8,
            'set': 9,
            'out': 10,
            'nov': 11,
            'dez': 12
        }
        self.__numbersOfMonthEnglish = {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr': 4,
            'may': 5,
            'jun': 6,
            'jul': 7,
            'aug': 8,
            'sep': 9,
            'oct': 10,
            'nov': 11,
            'dec': 12
        }

    def __dateTransformer(self, dateOld: str):
        '''
        Metodo de classe que formata datas no formato do Banco de Dados.
        Retorna uma string com a data formatada.
        '''
        try:
            nD: datetime
            if dateOld[3:6] in self.__numbersOfMonth:
                for k, v in self.__numbersOfMonth.items():
                    if k == dateOld[3:6]:
                        dO = dateOld.replace(k, str(v))
                        if int(dO[3:5].strip()) > 9:
                            dTStr = f'{dO[5:]}/{dO[3:5]}/{dO[:2]} 00:00:00'.strip()
                            nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
                        else:
                            dTStr = f'{dO[5:]}/{dO[3]}/{dO[:2]} 00:00:00'.strip()
                            nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
            else:
                for k, v in self.__numbersOfMonthEnglish.items():
                    if k == dateOld[3:6]:
                        dO = dateOld.replace(k, str(v))
                        if int(dO[3:5].strip()) > 9:
                            dTStr = f'{dO[5:]}/{dO[3:5]}/{dO[:2]} 00:00:00'.strip()
                            nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
                        else:
                            dTStr = f'{dO[5:]}/{dO[3]}/{dO[:2]} 00:00:00'.strip()
                            nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
            newDate = nD.strftime('%Y/%m/%d %H:%M:%S')
            return newDate
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__dateTransformer.__name__
            self.registerErrors(className, methName, e)

    def processedData(self, listTarget: list) -> None:
        '''
        Processa a lista com os dados agrupados por data.
        Os dados são salvos no atributo self.__dataProcessed.
        '''
        try:
            for groupData in listTarget:
                currentData: dict = {
                    'date': '',
                    'umidity': {
                        'minimum': float,
                        'maximum': float,
                        'mean': float,
                        'median': float,
                        'mode': float
                    },
                    'press': {
                        'minimum': float,
                        'maximum': float,
                        'mean': float,
                        'median': float,
                        'mode': float
                    },
                    'tempIndoor': {
                        'minimum': float,
                        'maximum': float,
                        'mean': float,
                        'median': float,
                        'mode': float
                    },
                    'tempOutdoor': {
                        'minimum': float,
                        'maximum': float,
                        'mean': float,
                        'median': float,
                        'mode': float
                    }
                }
                humidity: list = []
                press: list = []
                tempIndoor: list = []
                tempOutdoor: list = []

                for data in groupData[1]:
                    if 0 < data[0] <= 100:
                        humidity.append(data[0])
                    if 0 < data[1] <= 1000:
                        press.append(data[1])
                    if 0 < data[2] < 50:
                        tempIndoor.append(data[2])
                    if 0 < data[3] < 50:
                        tempOutdoor.append(data[3])

                currentData.update(
                    {'date': self.__dateTransformer(groupData[0])}
                )
                currentData.update({'umidity': {
                        'minimum': round(min(humidity), 2),
                        'maximum': round(max(humidity), 2),
                        'mean': round(mean(humidity), 2),
                        'median': round(median(humidity), 2),
                        'mode': round(mode(humidity), 2)
                    }})
                currentData.update({'press': {
                        'minimum': round(min(press), 2),
                        'maximum': round(max(press), 2),
                        'mean': round(mean(press), 2),
                        'median': round(median(press), 2),
                        'mode': round(mode(press), 2)
                    }})
                currentData.update({'tempIndoor': {
                        'minimum': round(min(tempIndoor), 2),
                        'maximum': round(max(tempIndoor), 2),
                        'mean': round(mean(tempIndoor), 2),
                        'median': round(median(tempIndoor), 2),
                        'mode': round(mode(tempIndoor), 2)
                    }})
                currentData.update({'tempOutdoor': {
                        'minimum': round(min(tempOutdoor), 2),
                        'maximum': round(max(tempOutdoor), 2),
                        'mean': round(mean(tempOutdoor), 2),
                        'median': round(median(tempOutdoor), 2),
                        'mode': round(mode(tempOutdoor), 2)
                    }})
                self.__dataProcessed.append(currentData)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.processedData.__name__
            self.registerErrors(className, methName, e)

    def getDataProcessed(self) -> list:
        '''
        Retorna o atributo self.__dataProcessed
        '''
        return self.__dataProcessed


class ConverterMonths(LogErrorsMixin):
    '''
        Converte os números dos meses em suas abreviações.
    '''
    def __init__(self) -> None:
        self.__numbersOfMonth = {
            '01': 'jan',
            '02': 'fev',
            '03': 'mar',
            '04': 'abr',
            '05': 'mai',
            '06': 'jun',
            '07': 'jul',
            '08': 'ago',
            '09': 'set',
            '10': 'out',
            '11': 'nov',
            '12': 'dez'
        }
        self.__numbersOfMonthEnglish = {
            '01': 'jan',
            '02': 'feb',
            '03': 'mar',
            '04': 'apr',
            '05': 'may',
            '06': 'jun',
            '07': 'jul',
            '08': 'aug',
            '09': 'sep',
            '10': 'oct',
            '11': 'nov',
            '12': 'dec'
        }

    def getMonths(self, numberOfMont: str):
        '''
            Informe uma string contendo o número correspondente ao mês.
            Retorna a abreviação do mês.
        '''
        if numberOfMont in self.__numbersOfMonth:
            return self.__numbersOfMonth[numberOfMont]


class DailyDate(LogErrorsMixin):
    '''Manipula datas.'''
    def __init__(self) -> None:
        self.__todayDate: datetime = datetime.now()

    def yesterdayDate(self) -> datetime:
        '''Retorna a data de ontem.'''
        return self.__todayDate - timedelta(1)

    def getTodayDate(self) -> datetime:
        '''Retorna o atributo __todayDate, contendo a data atual.'''
        return self.__todayDate

    def extractDay(self, date: datetime) -> str:
        '''Retorna o dia da data informada.'''
        dd = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcDay = dd.strftime('%d')
        return extratcDay

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
    # m = ConverterMonths()
    # print(m.getMonths('05'))

    bd = OperationDataBase('teste', dbCredentials(4))
    bd.insertCollumn(('J.Pereira',), collumn='nome')
    # bd.updateColumn(update='Juvenal', collumn='nome', condiction="codigo in (2, 4)")
