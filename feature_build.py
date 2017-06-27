# coding:utf-8
import numpy as np
import talib
import tushare as ts
from pandas import DataFrame, Series
from pymongo import MongoClient
import Quant as tech


class MarketBasicsFeatureEngineering:
    def __init__(self, start_day, ma_list, data_name):
        self.market_basics = ts.get_k_data('000001', start=start_day, index=True)
        self.ma_list = sorted(ma_list)
        self.data_name = data_name
        client = MongoClient('mongodb://localhost:27017/')
        db = client.stock
        self.collection = db.test

    def add_ma(self):
        self.market_basics = self.market_basics.set_index(['date'], drop=True)
        self.market_basics.sort_index()
        close_list = np.array(self.market_basics['close'])
        for ma in self.ma_list:
            temp_ma_list = [None] * (ma - 1)
            for i in range(len(close_list))[ma - 1:]:
                temp_ma_list.append(round(sum(close_list[i - ma + 1:i + 1]) / ma, 4))
            self.market_basics['ma_%d' % ma] = temp_ma_list
        for index in range(len(self.ma_list)):
            origin_arr = np.array(self.market_basics['ma_%d' % (self.ma_list[index])])
            for right_index in range(len(self.ma_list))[index + 1:]:
                comparison_arr = np.array(self.market_basics['ma_%d' % self.ma_list[right_index]])
                self.market_basics['ma_%d:ma_%d' % (self.ma_list[index], self.ma_list[right_index])] =\
                    ((comparison_arr / origin_arr) - 1) * 100

    def add_turn(self):
        """need history.py results"""
        # turn_df = pd.read_excel('turn.xlsx')
        # turn_df = turn_df.set_index(['date'], drop=True)
        # turn_se = turn_df['turn_average']
        # turn_se = turn_se.sort_index()
        # index_start = self.market_basics.index[0]
        # self.market_basics['turn_average'] = turn_se[index_start:]
        cursor = self.collection.find(no_cursor_timeout=True)
        turn_df = DataFrame()
        for document in cursor:
            stock_turn_dict = {key: document['date_index_data'][key]['turnover'] for key in document['date_index_data'].keys()}
            stock_turn_se = Series(stock_turn_dict)
            stock_turn_se.name = document['code']
            turn_df = turn_df.append(stock_turn_se)
        turn_df = turn_df.T
        turn_average = turn_df.mean(axis=1)
        self.market_basics['turn_average'] = turn_average

    def add_macd(self):
        fastperiod = 12
        slowperiod = 26
        signalperiod = 9
        macd, signal, hist = talib.MACD(np.array(self.market_basics['close']),
                                        fastperiod=fastperiod,
                                        slowperiod=slowperiod,
                                        signalperiod=signalperiod)
        self.market_basics['macd'] = macd
        self.market_basics['macd_signal'] = signal
        # macd会有前33个空值

    def add_price_change(self):
        close_list = list(self.market_basics['close'])
        # 防止涨跌幅过小，不用%表示
        price_change_list = [((close_list[i] - close_list[i-1]) / close_list[i-1]) * 100
                             for i in range(1, len(close_list))]
        price_change_list.insert(0, None)
        self.market_basics['price_change'] = price_change_list

    """ta-lib技法，详情见https://www.zhihu.com/question/39951384"""
    def talib_technique(self):
        technique = tech.TalibTechnique(self.market_basics)
        self.market_basics = technique.main()

    def operation_prepare(self):
        self.market_basics = self.market_basics[self.market_basics['macd'].notnull()]
        # macd变化率
        macd_arr = np.array(self.market_basics['macd'])
        macd_change = (macd_arr[1:] - macd_arr[:-1]) / macd_arr[:-1]
        macd_change = np.insert(macd_change, 0, np.nan)
        self.market_basics['macd_change'] = macd_change
        self.market_basics = self.market_basics[self.market_basics['macd_change'].notnull()]
        self.market_basics.drop(['code', 'volume', 'high', 'low', 'open', 'close'], inplace=True, axis=1)
        self.market_basics.to_excel(self.data_name, encoding='utf-8')

    def main(self):
        self.add_ma()
        self.add_turn()
        self.add_macd()
        self.add_price_change()
        self.talib_technique()
        self.operation_prepare()
        print(self.market_basics.head())

if __name__ == "__main__":
    prepare_file_name = "MFEPD.xlsx"  # "MarketFeatureEngineeringPrepareData"
    market = MarketBasicsFeatureEngineering("2014-02-10", ma_list=[8, 4, 6], data_name=prepare_file_name)
    market.main()
