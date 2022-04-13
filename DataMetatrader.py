# pip install opencv-python mysqlclient MetaTrader5 pandas pytz

import os
import cv2
import MySQLdb  # импортируем модуль для работы с БД MySql
import MetaTrader5 as mt5  # импортируем модуль для подключения к MetaTrader5
import pandas as pd  # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz  # импортируем модуль pytz для работы с таймзоной


class DataMetatrader():
    """A class for loading shares data from MetaTrader5"""

    def __init__(self, share_name):
        self.share_name = share_name
        self.conn = None
        self.cursor = None
        self.connection_to_db = False
        self.how_many_bars_max = 50000

        self.timezone = pytz.timezone("Etc/UTC")  # установим таймзону в UTC
        # создадим объект datetime в таймзоне UTC, чтобы не применялось смещение локальной таймзоны
        # self._utc_till = datetime.datetime.now(self.timezone)# datetime.datetime(2021, 10, 10, tzinfo=self.timezone)

    def ConnectToMetatrader5(self, path):
        mt5.initialize(path=path)
        # установим подключение к терминалу MetaTrader 5
        if not mt5.initialize():
            print("connection to MetaTrader5 failed, error code =", mt5.last_error())
            # завершим подключение к терминалу MetaTrader 5
            mt5.shutdown()
            quit()
        else:
            print("Connection to MetaTrader5: OK")

    def DisconnectFromMetatrader5(self):
        # Close connection
        if self.connection_to_db: self.conn.close(); print("Disconnection from db: OK")
        mt5.shutdown()
        print("Disconnection from MetaTrader5: OK")

    def ConnectToDb(self, host, user, passwd, db):
        try:
            self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
            self.cursor = self.conn.cursor()
            self.connection_to_db = True
            print("Connection to db: OK")
        except MySQLdb.Error as ex:
            print("connection to DB failed, error code =", ex)
            quit()

    def GetShareDataFromMetatraderRAW(self, ticket, timeframe, utc_till, how_many_bars, remove_last_bar=False):
        if timeframe not in ["D1", "H4", "H1", "M30", "M15", "M5", "M1"]: return "Error in timeframe"
        if timeframe == "D1":   timeframe = mt5.TIMEFRAME_D1
        if timeframe == "H4":   timeframe = mt5.TIMEFRAME_H4
        if timeframe == "H1":   timeframe = mt5.TIMEFRAME_H1
        if timeframe == "M30":  timeframe = mt5.TIMEFRAME_M30
        if timeframe == "M15":  timeframe = mt5.TIMEFRAME_M15
        if timeframe == "M5":   timeframe = mt5.TIMEFRAME_M5
        if timeframe == "M1":   timeframe = mt5.TIMEFRAME_M1
        rates = mt5.copy_rates_from(ticket, timeframe, utc_till, how_many_bars)
        # создадим из полученных данных DataFrame
        rates_frame = pd.DataFrame(rates)
        # сконвертируем время в виде секунд в формат datetime
        if len(rates_frame.index):
            rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
        if remove_last_bar: rates_frame = rates_frame[:-1]
        return rates_frame

    def GetShareDataFromMetatrader(self, ticket, timeframe, utc_till, how_many_bars, remove_last_bar, upper_heading=False):
        df = self.GetShareDataFromMetatraderRAW(ticket, timeframe, utc_till, how_many_bars, remove_last_bar)
        if upper_heading:
            df.rename(columns={"time": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "real_volume": "Volume"}, inplace=True)
        else:
            df.rename(columns={"time": "datetime", "real_volume": "volume"}, inplace=True)
        df = df.drop('tick_volume', 1)
        df = df.drop('spread', 1)
        return df

    def ExportToCsvFromMetatrader(self, ticket, timeframe, utc_till, how_many_bars, remove_last_bar, export_dir, prefix='', upper_heading=False):
        df = self.GetShareDataFromMetatrader(ticket, timeframe, utc_till, how_many_bars, remove_last_bar, upper_heading)
        if not os.path.exists(export_dir): os.makedirs(export_dir)
        df.to_csv(os.path.join(export_dir, prefix + ticket + "_" + timeframe + ".csv"), index=False, encoding='utf-8')

    def GetShareDataFromDb(self, ticket, timeframe, how_many_bars, upper_heading=False):
        if timeframe not in ["D1", "H4", "H1", "M30", "M15", "M5", "M1"]: return "Error in timeframe"
        table_name = ticket + "_" + timeframe
        self.cursor.execute(
            "SELECT time, open, high, low, close, volume FROM `" + table_name + "`" + " ORDER BY time DESC LIMIT " + str(
                how_many_bars)
        )
        # Get all data from table
        rows = self.cursor.fetchall()
        if upper_heading:
            dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
        else:
            dataframe = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index
        # print(dataframe.dtypes)
        return dataframe

    def ExportToCsvFromDb(self, ticket, timeframe, how_many_bars, export_dir, prefix='', upper_heading=False):
        df = self.GetShareDataFromDb(ticket, timeframe, how_many_bars, upper_heading)
        if not os.path.exists(export_dir): os.makedirs(export_dir)
        df.to_csv(os.path.join(export_dir, prefix + ticket + "_" + timeframe + ".csv"), index=False, encoding='utf-8')

    def always_get_share_data(self, ticket, timeframe):
        _timeframe = timeframe
        if timeframe not in ["D1", "H4", "H1", "M30", "M15", "M5", "M1"]: return "Error in timeframe"
        if timeframe == "D1":   timeframe = mt5.TIMEFRAME_D1
        if timeframe == "H4":   timeframe = mt5.TIMEFRAME_H4
        if timeframe == "H1":   timeframe = mt5.TIMEFRAME_H1
        if timeframe == "M30":  timeframe = mt5.TIMEFRAME_M30
        if timeframe == "M15":  timeframe = mt5.TIMEFRAME_M15
        if timeframe == "M5":   timeframe = mt5.TIMEFRAME_M5
        if timeframe == "M1":   timeframe = mt5.TIMEFRAME_M1

        how_many_bars = 0
        time_in_seconds_bar = 0

        if timeframe == mt5.TIMEFRAME_D1:   time_in_seconds_bar = 86400  # 60*60*24
        if timeframe == mt5.TIMEFRAME_H4:   time_in_seconds_bar = 14400  # 60*60*4
        if timeframe == mt5.TIMEFRAME_H1:   time_in_seconds_bar = 3600  # 60*60
        if timeframe == mt5.TIMEFRAME_M30:  time_in_seconds_bar = 1800  # 60*30
        if timeframe == mt5.TIMEFRAME_M15:  time_in_seconds_bar = 900  # 60*15
        if timeframe == mt5.TIMEFRAME_M5:   time_in_seconds_bar = 300  # 60*5
        if timeframe == mt5.TIMEFRAME_M1:   time_in_seconds_bar = 60  # 60

        table_name = ticket + "_" + _timeframe

        # ----------------------- UPDATE HISTORY -----------------------
        while True:
            # let's execute our query to db
            self.cursor.execute(
                "SELECT max(time) FROM `" + table_name + "`"
            )

            # Get all data from table
            rows = self.cursor.fetchall()
            last_bar_time = 0

            if rows[0][0] == None:
                how_many_bars = self.how_many_bars_max
            else:
                last_bar_time = rows[0][0]
                print(last_bar_time)

                # calc missed bars
                today = datetime.datetime.now()
                num_bars_to_load = ((today - last_bar_time).total_seconds()) // time_in_seconds_bar + 1
                print(num_bars_to_load)

                how_many_bars = int(num_bars_to_load)

            # получим данные по завтрашний день
            utc_till = datetime.datetime.now() + datetime.timedelta(days=1)
            print(utc_till)
            rates = mt5.copy_rates_from(ticket, timeframe, utc_till, how_many_bars)

            # создадим из полученных данных DataFrame
            rates_frame = pd.DataFrame(rates)
            # сконвертируем время в виде секунд в формат datetime
            if len(rates_frame.index):
                rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            # выведем данные
            print("\nВыведем датафрейм с данными")
            print(rates_frame)

            for i in range(len(rates_frame.index) - 1):  # последний бар не берем -1 т.к. он еще формируется
                _time = rates_frame.at[i, "time"]
                _open = rates_frame.at[i, "open"]
                _high = rates_frame.at[i, "high"]
                _low = rates_frame.at[i, "low"]
                _close = rates_frame.at[i, "close"]
                _tick_volume = rates_frame.at[i, "tick_volume"]
                _real_volume = rates_frame.at[i, "real_volume"]
                print(i, _time, _open, _high, _low, _close, _tick_volume, _real_volume)

                if ((rows[0][0] != None) and (_time > last_bar_time)) or ((rows[0][0] == None)):
                    # let's insert row in table
                    self.cursor.execute(
                        "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, tick_volume) "
                                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (_time, _open, _high, _low, _close, _real_volume, _tick_volume))

            # to commit changes to db!!!
            # run this command:
            self.conn.commit()

            last_bar_time = rates_frame.at[len(rates_frame.index) - 1, "time"]
            print(last_bar_time)

            next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar)
            print(next_bar_time)

            if next_bar_time > datetime.datetime.now():
                break

        # ----------------------- Update in Real Time -----------------------
        while True:
            next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar)
            wait_for_calculated = int((next_bar_time - datetime.datetime.now()).total_seconds())
            print("Last bar time: %s Next bar time: %s" % (last_bar_time, next_bar_time))
            print("waiting %s seconds..." % (wait_for_calculated))

            # cv2.waitKey(abs(wait_for_calculated*1000+500)) # 500 milsec delay
            for sec in range(abs(wait_for_calculated)):
                if ((sec + 1) % 30 == 0):
                    print(wait_for_calculated - sec)
                else:
                    print(wait_for_calculated - sec, end=" ")
                cv2.waitKey(1000)

            # add new data to table
            # print(datetime.datetime.now())
            print("Last bar time: %s Next bar time: %s" % (last_bar_time, next_bar_time))
            # check_last_bar_writed_to_db = get_last_bar_time(cursor)
            # print(check_last_bar_writed_to_db)
            # if (last_bar_time == check_last_bar_writed_to_db):
            #     print("Ok")
            # else:
            #     print("Failed write to DB!")
            # ...

            # calc missed bars
            today = datetime.datetime.now()
            num_bars_to_load = ((
                                            today - last_bar_time).total_seconds()) // time_in_seconds_bar + 5  # берем +5 бар назад
            print(num_bars_to_load)

            how_many_bars = int(num_bars_to_load)

            # получим данные по завтрашний день
            utc_till = datetime.datetime.now() + datetime.timedelta(days=1)
            print(utc_till)

            # exit(1)

            check_we_have_next_bar_loaded = False
            while not check_we_have_next_bar_loaded:
                rates = mt5.copy_rates_from(ticket, timeframe, utc_till, how_many_bars)

                # создадим из полученных данных DataFrame
                rates_frame = pd.DataFrame(rates)
                # сконвертируем время в виде секунд в формат datetime
                if len(rates_frame.index):
                    rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

                # проверка, что есть данные следующей свечи
                for i in range(len(rates_frame.index)):
                    _time = rates_frame.at[i, "time"]
                    if _time > last_bar_time:
                        check_we_have_next_bar_loaded = True
                        print("We have got next bar from Metatrader")
                    else:
                        print("Will try again - to get next bar ... ")
                        cv2.waitKey(500)  # 500 milsec delay

            # выведем данные
            print("\nВыведем датафрейм с данными")
            print(rates_frame)

            for i in range(len(rates_frame.index)):
                _time = rates_frame.at[i, "time"]
                _open = rates_frame.at[i, "open"]
                _high = rates_frame.at[i, "high"]
                _low = rates_frame.at[i, "low"]
                _close = rates_frame.at[i, "close"]
                _tick_volume = rates_frame.at[i, "tick_volume"]
                _real_volume = rates_frame.at[i, "real_volume"]
                print(i, _time, _open, _high, _low, _close, _tick_volume, _real_volume)

                if _time >= last_bar_time and _time < next_bar_time:
                    # let's insert row in table
                    self.cursor.execute(
                        "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, tick_volume) "
                                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (_time, _open, _high, _low, _close, _real_volume, _tick_volume))

            # to commit changes to db!!!
            # run this command:
            self.conn.commit()

            last_bar_time = next_bar_time
        # ----------------------- Update in Real Time -----------------------

        pass
