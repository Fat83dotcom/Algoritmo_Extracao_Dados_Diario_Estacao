import os
import csv
import psycopg2
from abc import ABC
from pathlib import Path
from itertools import groupby
from databaseSettings import CONFIG


class DataBase(ABC):
    def __init__(
            self, host='', port='', dbname='', user='', password=''
            ) -> None:
        self.con = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password)
        self.cursor = self.con.cursor()

    def closeConnection(self):
        self.con.close()

    def toExecute(self, sql):

        self.cursor.execute(sql)

    def toExecuteMogrify(self, sql):
        self.cursor.mogrify(sql)

    def toSend(self):
        self.con.commit()

    def toAbort(self):
        self.con.rollback()

    def seekData(self):
        return self.cursor.fetchall()

    def seekOneData(self):
        return self.cursor.fetchone()

    def seekInterval(self, intervalo):
        return self.cursor.fetchmany(intervalo)

    def generatorSQLInsert(self, *args, colunm_names=None,  table_name=None):
        values = args[0]
        if len(values) == 1:
            values = str(values).replace(',', '')
        sql = "INSERT INTO %s %s VALUES %s" % (
            table_name, colunm_names, values
        )
        return sql

    def generatorSQLUpdate(
            self, *args, collumn_name=None, table_name=None, condiction=None
            ):
        valores = args[0]
        sql = "UPDATE %s SET %s='%s' WHERE %s" % (
            table_name, collumn_name, valores, condiction
        )
        return sql


class OperationDataBase(DataBase):

    def __init__(self, table: str) -> None:
        self.__table = table
        self.Bd = DataBase(
            dbname=CONFIG['banco_dados'],
            user=CONFIG['usuario'],
            port=CONFIG['porta'],
            password=CONFIG['senha'],
            host=CONFIG['host']
        )

    def updateColumn(self, collumn, condiction, update):
        sql = self.generatorSQLUpdate(
            update, table_name=self.__table,
            collumn_name=collumn, condiction=condiction)
        try:
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def insertCollumn(self, *args, collumn):
        try:
            sql = self.generatorSQLInsert(
                *args, colunm_names=collumn, table_name=self.__table
            )
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def insertCollumnMogrify(self, *args, collumn):
        try:
            sql = self.generatorSQLInsert(
                *args, colunm_names=collumn, table_name=self.__table
            )
            self.Bd.toExecuteMogrify(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def closeConnection(self):
        return self.Bd.closeConnection()

    def toExecute(self, sql):
        return self.Bd.toExecute(sql)


class FileRetriever:
    def __init__(self, pathTarget, extension='.csv') -> None:
        self.__foundFiles: list = []
        self.__pathTarget = pathTarget
        self.__extensionFile = extension

    def __findFiles(self) -> None:
        for root, _, file_ in os.walk(self.__pathTarget):
            for targetFile in file_:
                if self.__extensionFile in targetFile:
                    self.__foundFiles.append(os.path.join(root, targetFile))

    def findOneFile(self, fileName: str):
        for root, _, file_ in os.walk(self.__pathTarget):
            for targetFile in file_:
                if fileName in targetFile:
                    return str(os.path.join(root, targetFile))
                else:
                    return 'Arquivo não encontrado.'

    def generatorPathTargetFile(self, month, year):
        try:
            pathFile = os.path.join(
                self.__pathTarget,
                f'{month}_{year}_Modlog{self.__extensionFile}'
            )
            return pathFile
        except Exception as e:
            print(e.__class__.__name__, e)

    def getFoundFiles(self):
        try:
            self.__findFiles()
            if self.__foundFiles:
                for files in self.__foundFiles:
                    yield files
            else:
                raise Exception('Arquivos não encontrdos')
        except Exception as e:
            print(e)


class DataExtractor:
    def __init__(self) -> None:
        self.__extractData: list = []

    def dataExtract(self, file: list) -> None:
        try:
            def __extractKey(listTarget):
                return listTarget[0][:11]

            PATH_CSV = Path(__file__).parent / file  # type: ignore
            with open(PATH_CSV, 'r', encoding='utf-8') as myCsv:
                reader = csv.reader((line.replace('\0', '') for line in myCsv))
                groups = groupby(reader, key=__extractKey)
                for date, data in groups:
                    self.__extractData.append((date, (
                        (
                            float(val[1]),
                            float(val[2]),
                            float(val[3]),
                            float(val[4])
                        )
                        if
                        val[1] and val[2] and val[3] and val[4] != ''
                        else (0, 0, 0, 0)
                        for val in data
                    )))
        except (IndexError, Exception) as e:
            raise e

    def getExtractData(self) -> list:
        return self.__extractData
