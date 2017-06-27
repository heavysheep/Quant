# coding:utf-8
import pandas as pd
import tushare as ts
import os
import gc
import multiprocessing


class DataUpdateToExcel:
    """将数据存储在本地的excel表中"""
    def __init__(self, folder_name='stock_set', suffix_format='xlsx'):
        stock_df = ts.get_stock_basics()
        temp_for_latest = ts.get_k_data('000001')
        self.stock_list = list(stock_df.index)
        self.path = r'%s\%s' % (os.getcwd(), folder_name)
        self.format = suffix_format
        self.latest_data = sorted(list(temp_for_latest['date']), reverse=True)[0]
        self.start_day = "2014-02-10"

    def stock_index_files(self):
        pool = multiprocessing.Pool(processes=3)
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        pool.map(self.stock_index_main, self.stock_list)
        pool.close()
        pool.join()

    def stock_index_main(self, code):
        # 虽然直接覆写新数据覆盖最简单最快，考虑再三，还是补充数据比较稳妥
        if code + '.' + self.format in os.listdir(self.path):
            df_old = pd.read_excel(self.path + '\\' + code + '.' + self.format)
            if self.latest_data not in df_old.index:
                df_new = ts.get_hist_data(code, ktype="D", start=self.start_day)
                df_old.set_index(['date'], drop=True, inplace=True)
                df_index = list(set(df_old.index) ^ set(df_new.index))
                if not df_index:
                    print(code, "无更新内容")
                    return
                df_old = df_old.append(df_new.loc[df_index])
                df_old = df_old.sort_index(ascending=False)
                print(code, "更新:", ''.join(df_index))
                df_old.to_excel(self.path + '\\' + code + '.' + self.format)
        else:
            df_new = ts.get_hist_data(code, ktype="D", start=self.start_day)
            try:
                df_new = df_new.sort_index(ascending=False)
            except AttributeError:
                print("%s出现问题，原因:%s" % (code, df_new))
                pass
            else:
                df_new.to_excel(self.path + '\\' + code + '.' + self.format)
        gc.collect()

    def sotck_big_table(self):
        """
        need index_files results
        excel部分停工，换成mongodb存储数据
        """

if __name__ == "__main__":
    data = DataUpdateToExcel()
    data.stock_index_files()
