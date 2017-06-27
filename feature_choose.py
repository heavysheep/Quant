# coding:utf-8
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.ensemble import AdaBoostRegressor
import pandas as pd
import numpy as np


DF = pd.read_excel('MFEPD.xlsx', index_col='date')
xData = DF.drop(['price_change'], axis=1)
yData = DF['price_change']
# 删除0方差无用特征
zero_variance_array = np.array(xData.apply(lambda x:x.var() != 0, axis=0))
xData = xData[xData.columns[zero_variance_array]]
# 特征缩放
min_max_scaler = preprocessing.MinMaxScaler()
xData = min_max_scaler.fit_transform(xData)


X_train, X_test, y_train, y_test = train_test_split(xData, yData, train_size=0.7)
adaboost = AdaBoostRegressor()
adaboost.fit(X_train, y_train)
print(adaboost.score(X_train, y_train))
result = adaboost.predict(X_test)
accuracy = ((np.array(y_test) * result) > 0)
print(list(accuracy).count(True) / len(accuracy))


