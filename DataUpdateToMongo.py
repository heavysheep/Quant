# coding:utf-8
import gc
import json
import tushare as ts
from pymongo import MongoClient


class DataUpdateToMongo:
    def __init__(self):
        # 配置本地mongo
        client = MongoClient('mongodb://localhost:27017/')
        db = client.stock
        self.collection = db.test
        stock_df = ts.get_stock_basics()
        self.history_start_day = "2014-02-10"
        temp_for_time_index = ts.get_hist_data('000001', start=self.history_start_day)
        self.time_index = sorted(list(temp_for_time_index.index))
        self.stock_list = list(stock_df.index)

    def stock_index_files(self):
        # 待解决TypeError: can't pickle _thread.lock objects，大概是因为mongo代码存在线程锁
        # pool = multiprocessing.Pool(processes=3)
        # pool.map(self.stock_index_main, self.stock_list)
        # pool.close()
        # pool.join()
        count = 0
        for code in self.stock_list:
            self.stock_index_main(code, count)
            count += 1

    def stock_index_main(self, code, count):
        cursor = self.collection.find_one({"code": code})  # no_cursor_timeout=True
        if cursor:
            if self.time_index[-1] not in cursor['date_index_data'].keys():
                stock_data = self.dataframe_join(code)
                miss_key = list(set(stock_data['date_index_data'].keys() - set(cursor['date_index_data'].keys())))
                for key in miss_key:
                    self.collection.update_one({'_id': cursor['_id']}, {'$set': {'date_index_data.%s' % key: stock_data['date_index_data'][key]}})
                print(count, code, '正在添加', len(miss_key), '条数据：', miss_key)
            else:
                print(code, count, '无更新内容')
        else:
            stock_data = self.dataframe_join(code)
            if not stock_data:
                return
            print(count, code, '新建表')
            self.collection.insert_one(stock_data)
        gc.collect()

    def dataframe_join(self, code):
        df_k = ts.get_k_data(code, ktype="D", start=self.history_start_day)
        try:
            df_k.set_index(['date'], drop=True, inplace=True)
            df_k = df_k.drop(['code'], axis=1)
            df_hist = ts.get_hist_data(code, ktype="D", start=self.history_start_day)
            df_hist = df_hist.loc[:, 'ma5':'turnover']
        except AttributeError:
            print("%s出现问题，原因:%s" % (code, df_k))
            return
        df_outer = df_k.join(df_hist)
        stock_data = {
            "code": code,
            "date_index_data": json.loads(df_outer.T.to_json())
        }
        return stock_data

    # def time_index_files(self):
    #     """need stock_index_main results"""
    #     print(self.time_index)

if __name__ == "__main__":
     data = DataUpdateToMongo()
     data.stock_index_files()
