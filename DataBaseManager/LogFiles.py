import os
import time
from datetime import datetime


class LogFiles:
    def registerTimeElapsed(self, timeInit: float, timeEnd: float):
        totalTime = timeEnd - timeInit
        self.recordFile({'time elapsed: ': totalTime, 'Seconds': ''})

    def snapshotTime(self):
        return time.time()

    def recordFile(self, *args):
        path: str = os.path.join(os.getcwd(), 'logFile.txt')
        with open(path, "a", encoding='utf-8') as file:
            file.write(f'{args[0]}\n')

    def registerTimeLogStart(self):
        register = {
            'dialog': '*** Inicio do processo ***',
            'startTime': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        }
        self.recordFile(register)

    def registerTimeLogEnd(self):
        register = {
            'dialog': '*** Final do processo ***',
            'endTime': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        }
        self.recordFile(register)

    def registerErros(self, className, methName, error):
        register = {
            'className': className,
            'methName': methName,
            'error': error,
            'hour': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        }
        self.recordFile(register)
