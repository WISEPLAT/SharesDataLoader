# pip install opencv-python mysqlclient MetaTrader5 pandas pytz
# git clone https://github.com/WISEPLAT/SharesDataLoader

import datetime
from SharesDataLoader.DataMetatrader import DataMetatrader

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    ticker = "GAZP"
    prefix = 'TQBR.'
    timeframe = "D1"
    how_many_bars = 50000

    # получим данные по завтрашний день
    utc_till = datetime.datetime.now() + datetime.timedelta(days=1)
    print(utc_till)

    load_data = DataMetatrader()
    load_data.ConnectToMetatrader5(path=f"C:\Program Files\FINAM MetaTrader 5\terminal64.exe")

    # data = load_data.GetShareDataFromMetatrader(ticker=ticker, timeframe=timeframe, utc_till=utc_till, how_many_bars=how_many_bars, remove_last_bar=True, upper_heading=False)
    # print(data)

    # create CSV file
    load_data.ExportToCsvFromMetatrader(ticker=ticker, timeframe=timeframe, utc_till=utc_till, how_many_bars=how_many_bars, remove_last_bar=True, export_dir="csv", prefix=prefix, upper_heading=False)

    # # work with DB
    # load_data.ConnectToDb( host="192.168.0.200",
    #                         user="sharesuser",
    #                         passwd="SomePassword123",
    #                         db="shares")
    # #load_data.always_get_share_data(ticker=ticker, timeframe=timeframe)
    #
    # # data = load_data.GetShareDataFromDb(ticker, timeframe, how_many_bars, upper_heading=False)
    # # print(data)
    # load_data.ExportToCsvFromDb(ticker, timeframe, how_many_bars, export_dir="csv", prefix=prefix, upper_heading=True)
