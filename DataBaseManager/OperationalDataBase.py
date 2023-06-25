import psycopg
from abc import ABC
from DataBaseManager.LogFiles import LogFiles
from DataBaseManager.collumnTables import dado_diario


class DataBase(ABC, LogFiles):
    '''Classe abstrata que fornece os serviços básicos
    para as operações do banco de dados'''
    def __init__(self, dBConfig: dict) -> None:
        self.host: str = dBConfig['host']
        self.port: str = dBConfig['port']
        self.dbname: str = dBConfig['dbname']
        self.user: str = dBConfig['user']
        self.password: str = dBConfig['password']

    def toExecute(self, sql):
        try:
            with psycopg.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                port=self.port,
                password=self.password
            ) as con:
                with con.cursor() as cursor:
                    sql, data = sql
                    cursor.execute(sql, data)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.toExecute.__name__
            self.registerErros(className, methName, e)

    def placeHolderSQLGenerator(self, values) -> str | None:
        try:
            placeHolders: str = ''
            sizeValues = len(values)
            for n, _ in enumerate(values):
                if sizeValues == 1 or n == (sizeValues - 1):
                    placeHolders += '%s'
                else:
                    placeHolders += '%s, '
            return placeHolders
        except Exception as e:
            className = self.__class__.__name__
            methName = self.placeHolderSQLGenerator.__name__
            self.registerErros(className, methName, e)

    def SQLInsertGenerator(self, *args, colunm_names=None,  table_name=None):
        try:
            values = args[0]
            pHolders = self.placeHolderSQLGenerator(values)
            sql = (
                f"INSERT INTO {table_name} ({colunm_names}) VALUES ({pHolders})",
                values
            )
            return sql
        except Exception as e:
            className = self.__class__.__name__
            methName = self.SQLInsertGenerator.__name__
            self.registerErros(className, methName, e)

    def SQLUpdateGenerator(
            self, *args, collumn_name=None, table_name=None, condiction=None
            ):
        try:
            values = args
            pHolders = self.placeHolderSQLGenerator(values)
            sql = (
                f"UPDATE {table_name} SET {collumn_name}=({pHolders}) WHERE {condiction}",
                values
            )
            return sql
        except Exception as e:
            className = self.__class__.__name__
            methName = self.SQLUpdateGenerator.__name__
            self.registerErros(className, methName, e)


class OperationDataBase(DataBase, LogFiles):
    '''Realiza as operações com o PostgreSQL'''
    def __init__(self, table: str, dBConfig: dict) -> None:
        self.__table = table
        super().__init__(dBConfig)

    def updateColumn(self, collumn, condiction, update):
        '''
            Atualiza colunas.
            Parametros: collumn -> Nome da coluna
            condition -> Condição de atualização
            update -> Valor da modificação
        '''
        try:
            sql = self.SQLUpdateGenerator(
                update, table_name=self.__table,
                collumn_name=collumn, condiction=condiction)
            self.toExecute(sql)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.updateColumn.__name__
            self.registerErros(className, methName, e)

    def insertCollumn(self, *args, collumn):
        '''
            Insere dados na tabela.
            Parametros:
            *args -> tupla com os valores, em ordem com a coluna
            collumn -> Nome das colunas, na ordem de inserção.
        '''
        try:
            sql = self.SQLInsertGenerator(
                *args, colunm_names=collumn, table_name=self.__table
            )
            self.toExecute(sql)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.insertCollumn.__name__
            self.registerErros(className, methName, e)


class DataModel(LogFiles):
    '''Modelo dos dados do banco'''
    def __init__(self, dB: OperationDataBase) -> None:
        self.DBInstance = dB

    def executeDB(self, iterable: list) -> None:
        '''
            Insere os dados extraidos no modelo do BD.
            Retorna -> None
        '''
        for dataDays in iterable:
            try:
                # self.DBInstance.toExecute('SET datestyle to ymd', ())
                self.DBInstance.insertCollumn(
                    (dataDays['date'],
                        dataDays['umidity']['mean'],
                        dataDays['umidity']['minimum'],
                        dataDays['umidity']['maximum'],
                        dataDays['umidity']['median'],
                        dataDays['umidity']['mode'],
                        dataDays['press']['mean'],
                        dataDays['press']['minimum'],
                        dataDays['press']['maximum'],
                        dataDays['press']['median'],
                        dataDays['press']['mode'],
                        dataDays['tempIndoor']['mean'],
                        dataDays['tempIndoor']['minimum'],
                        dataDays['tempIndoor']['maximum'],
                        dataDays['tempIndoor']['median'],
                        dataDays['tempIndoor']['mode'],
                        dataDays['tempOutdoor']['mean'],
                        dataDays['tempOutdoor']['minimum'],
                        dataDays['tempOutdoor']['maximum'],
                        dataDays['tempOutdoor']['median'],
                        dataDays['tempOutdoor']['mode']),
                    collumn=dado_diario
                )
            except Exception as e:
                className = self.__class__.__name__
                methName = self.executeDB.__name__
                self.registerErros(className, methName, e)