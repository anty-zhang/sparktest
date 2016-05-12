# -*- coding: utf-8 -*-
__author__ = 'andy'

"""
例子，美国 1880 － 2014 年新生婴儿数据统计

    目标：用美国 1880 － 2014 年新生婴儿的数据来做做简单的统计
    数据源： https://catalog.data.gov
    数据格式：
        每年的新生婴儿数据在一个文件里面
        每个文件的每一条数据格式：姓名,性别,新生人数

"""
import operator
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark import SparkContext


# spark udf(User Defined Functions)

def map_extract(element):
    file_path, content = element
    year = file_path[-8:-4]
    print year
    return [(year, i) for i in content.split('\r\n') if i]


# spark logic
sc = SparkContext('local', 'american_child')
res = sc.wholeTextFiles('/Users/guoqiangzhang/PycharmProjects/sparktest/data/child',
                        minPartitions=40) \
        .map(map_extract)                   \
        .flatMap(lambda x: x)               \
        .map(lambda x: (x[0], int(x[1].split(',')[2])))    \
        .reduceByKey(operator.add)                          \
        .collect()

# result displaying

data = pd.DataFrame.from_records(res, columns=['year', 'birth'])    \
        .sort(columns=['year'], ascending=True)

ax = data.plot(x=['year'], y=['birth'], figsize=(20, 6),
               title='US Baby Birth Data from 1980 to 2015',
               linewidth=3)
ax.set_axis_bgcolor('white')
ax.grid(color='gray', alpha=0.2, axis='y')

plt.show()




