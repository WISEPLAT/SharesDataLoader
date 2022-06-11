# pip install backtrader mysqlclient pandas
# git clone https://github.com/cia76/BackTraderQuik
# git clone https://github.com/cia76/QuikPy

from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK

import os
#import cv2 #pip install opencv-python
import MySQLdb  # импортируем модуль для работы с БД MySql
import pandas as pd  # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz  # импортируем модуль pytz для работы с таймзоной


class DataQuik():
    """A class for loading shares data from MetaTrader5"""

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connection_to_db = False
        self.how_many_bars_max = 50000

        self.timezone = pytz.timezone("Etc/UTC")  # установим таймзону в UTC
        # создадим объект datetime в таймзоне UTC, чтобы не применялось смещение локальной таймзоны
        # self._utc_till = datetime.datetime.now(self.timezone)# datetime.datetime(2021, 10, 10, tzinfo=self.timezone)

    def GetShareDataFromQuik(self, qpProvider, ticker, timeframe, utc_till, how_many_bars, remove_last_bar, upper_heading=False):
        """
        Получаем данные с Quik в виде dateframe
        upper_heading формирует названия колонок:
        # upper_heading == False        ==>     datetime,open,high,low,close,volume
        # upper_heading == "Date"       ==>     Date,Open,High,Low,Close,Volume
        # upper_heading == "TSLab"      ==>     <DATA>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
        """
        part_symbol = ticker.split(".")
        classCode = part_symbol[0]  # Класс тикера
        secCode = part_symbol[1]  # Тикер

        interval = 1  # Для минутных временнЫх интервалов ставим кол-во минут
        if timeframe == 'M1': interval = 1
        if timeframe == 'M5': interval = 5
        if timeframe == 'M15': interval = 15
        if timeframe == 'M30': interval = 30
        if timeframe == 'H1': interval = 60
        if timeframe == 'H2': interval = 120
        if timeframe == 'H4': interval = 240
        _tf = interval
        if timeframe == 'D1': interval = 1440; _tf = "D"  # Дневной временной интервал # В минутах
        if timeframe == 'W1': interval = 10080; _tf = "W"  # Недельный временной интервал # В минутах
        if timeframe == 'MN1':  interval = 23200; _tf = "MN"  # Месячный временной интервал # В минутах

        newBars = qpProvider.GetCandlesFromDataSource(classCode, secCode, interval, 0)[
            "data"]  # Получаем все свечки
        if remove_last_bar:  # Для дневных баров мы получаем еще несформировавшийся бар текущей сессии. Он нам не нужен
            newBars = newBars[:len(newBars) - 1]  # Берем все бары кроме последнего
        pdBars = pd.DataFrame.from_dict(pd.json_normalize(newBars),
                                        orient='columns')  # Внутренние колонки даты/времени разворачиваем в отдельные колонки
        pdBars.rename(columns={'datetime.year': 'year', 'datetime.month': 'month', 'datetime.day': 'day',
                               'datetime.hour': 'hour', 'datetime.min': 'minute', 'datetime.sec': 'second'},
                      inplace=True)  # Чтобы получить дату/время переименовываем колонки
        pdBars.index = pd.to_datetime(
            pdBars[['year', 'month', 'day', 'hour', 'minute', 'second']])  # Собираем дату/время из колонок
        pdBars = pdBars[['open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки
        pdBars.index.name = 'datetime'  # Ставим название индекса даты/времени
        pdBars.volume = pd.to_numeric(pdBars.volume, downcast='integer')  # Объемы могут быть только целыми

        if upper_heading == "Date":
            pdBars.index.name = 'Date'  # Ставим название индекса даты/времени
            pdBars.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)

        pdBars.reset_index(inplace=True)

        if upper_heading == "TSLab":
            # https://doc.tslab.pro/tslab/postavshiki-dannykh/servera-istorii/tekstovye-faily-i-offlain-postavshiki-dannykh/tekstovye-faily-s-istoricheskimi-dannymi

            # pdBars.reset_index(inplace=True)
            # print(pdBars, len(pdBars), pdBars.dtypes)
            # for d in pdBars['datetime']:
            #     pdBars['date'] = d.date()
            #     pdBars['time'] = d.time()

            new_col_date = []
            new_col_time = []

            for i in range(len(pdBars)):
                data_date = pdBars.datetime[i]  # берем дату # print(data.index[0])
                _date = data_date.date()  # дата
                _time = data_date.time()  # время
                # print(data_date, _date, _time)
                new_col_date.append(_date)
                new_col_time.append(_time)

            pdBars.insert(0, '<TICKER>', secCode)
            pdBars.insert(1, '<PER>', _tf)
            pdBars.insert(2, '<DATA>', new_col_date)
            pdBars.insert(3, '<TIME>', new_col_time)
            pdBars = pdBars.drop('datetime', 1)

            pdBars.rename(columns={"open": "<OPEN>", "high": "<HIGH>", "low": "<LOW>", "close": "<CLOSE>", "volume": "<VOL>"}, inplace=True)

        return pdBars

    def ExportToCsvFromQuik(self, qpProvider, ticker, timeframe, utc_till, how_many_bars, remove_last_bar, export_dir, prefix='', upper_heading=False):
        """
        Экспортируем полученные данные с Quik в CSV
        upper_heading формирует названия колонок:
        # upper_heading == False        ==>     datetime,open,high,low,close,volume
        # upper_heading == "Date"       ==>     Date,Open,High,Low,Close,Volume
        # upper_heading == "TSLab"      ==>     <DATA>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
        """
        df = self.GetShareDataFromQuik(qpProvider, ticker, timeframe, utc_till, how_many_bars, remove_last_bar, upper_heading)
        if not os.path.exists(export_dir): os.makedirs(export_dir)
        df.to_csv(os.path.join(export_dir, prefix + ticker + "_" + timeframe + ".csv"), index=False, encoding='utf-8')

    def ConnectToDb(self, host, user, passwd, db):
        try:
            self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
            self.cursor = self.conn.cursor()
            self.connection_to_db = True
            print("Connection to db: OK")
        except MySQLdb.Error as ex:
            print("connection to DB failed, error code =", ex)
            quit()
