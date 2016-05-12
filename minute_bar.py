# -*- coding: utf-8 -*-
__author__ = 'andy'

import json
import pandas as pd
from pyspark import SparkContext
# import sklearn.preprocessing
import matplotlib.pyplot as plt


# create sc
sc = SparkContext('local', 'minute_bar')

# load data
path = '/Users/guoqiangzhang/PycharmProjects/sparktest/data/minute_bar'
rdd_mkt_data = sc.wholeTextFiles(path, minPartitions=80)    \
                .setName('index_minute_bar')                \
                .cache()
"""
# deal data
"""
# 1.UDF, 从rdd_mkt_data获取指定要预测的分钟线


def minute_bar_index(line_id):
    line_data = rdd_mkt_data.filter(lambda x: line_id in x[0]).collect()
    line = pd.DataFrame.from_dict(json.loads(line_data[0][1]))
    line.sort(columns=['barTime'], ascending=True, inplace=True)
    return line


# 指定想要预测的线的 id，这里我们预测上证指数 2016.03.17 的分钟线
target_line = '000001.ZICN-20160317'
# 指定用于计算相似度的分钟线长度，这里我们用 90 个分钟 bar，
# 即开盘 09:30 到 11:00 的分钟线
minute_bar_length = 90
minute_bar_length_share = sc.broadcast(minute_bar_length)
target_line_mkt_data = minute_bar_index(target_line)
target_line_share = sc.broadcast(target_line_mkt_data)

# 2. 计算相似度


def cal_similarity(line):
    """
        计算相似度
        参数 line 的格式是： (line_id, line_data)
    """
    # import pandas as pdd
    # 使用sklearn，pandas简化计算

    # 通过广播变量获取预测的目标线和准备用来预测的分钟线长度
    minute_length = minute_bar_length_share.value
    target_line = target_line_share.value

    line_id, line_data = line

    # 获取 pandas dataframe 格式的某日分钟线行情
    ticker, tradeDate = line_id[-25:-5].split('-')
    line_data = pd.DataFrame.from_dict(json.loads(line_data))
    line_data.sort(columns=['barTime'], ascending=True, inplace=True)

    # 每天有 240 条分钟线的 bar，我们用 前 minute_length 来计算相似度
    line1 = list(target_line.ratio)[:minute_length]
    line2 = list(line_data.ratio)[:minute_length]

    tmp = pd.DataFrame()
    tmp['first'], tmp['second'] = line1, line2
    tmp['diff'] = tmp['first'] - tmp['second']
    diff_square = sum(tmp['diff'] ** 2)

    # 返回格式：(分钟线id，该分钟线和目标线前 minute_length 个长度的相似度)
    return line_id[-25:-5], round(diff_square, 5)

# 3. spark相似度计算代码
rdd_similarity = rdd_mkt_data.map(cal_similarity)   \
                    .setName('rdd_similarity')      \
                    .cache()

"""
# result displaying
"""
# 1. 获取相似度高的分钟线
# UDF 从rdd_mkt_data中获取指定的多日分钟线数据


def get_similarity_line(similarity_data):
    # 获取原始相似的分钟线数据
    rdd_lines = rdd_mkt_data.filter(
        lambda x: x[0][-25:-5] in [i[0] for i in similarity_data]
    ).collect()

    # 把原始分钟线数据转成 pandas dataframe 格式
    similar_line = {
        x[0][-25:-5]: pd.DataFrame.from_dict(json.loads(x[1])) for x in rdd_lines
    }

    # TODO inplace=True ???
    similar_line = {
        x: similar_line[x].sort(columns=['barTime'], ascending=True) for x in similar_line
    }
    return similar_line


# 获取相似度最高的30日分钟线
similarity_data = rdd_similarity.takeOrdered(30, key=lambda x: x[1])
similarity_line = get_similarity_line(similarity_data)


# 2. 根据相似分钟线绘制预测图


def draw_similarity(target_line, minute_bar_line, similar_data):
    res = pd.DataFrame()

    columns = []
    for i in similar_data:
        line_id = i[0]
        line_data = similarity_line[line_id]
        res[line_id] = line_data.ratio
        if 'minute' not in res:
            res['minute'] = line_data.barTime

        columns.append(line_id)

    res['fitting'] = res[columns].sum(axis=1) / len(columns)
    res['target_line'] = target_line_mkt_data.ratio

    print res

    # plot
    ax = res.plot(x='minute', y=columns, figsize=(20, 13),
                  legend=False, title=u'Minute Bar Prediction')
    res.plot(y=['target_line'], ax=ax, linewidth=5, style='.b')
    res.plot(y=['fitting'], ax=ax, linewidth=4, style='-y')
    ax.vlines(x=minute_bar_length, ymin=-0.02, ymax=0.02,
              linestyles='dashed')
    ax.set_axis_bgcolor('white')
    ax.grid(color='gray', alpha=0.2, axis='y')

    # plot area
    avg_line = res['fitting']
    avg_line = list(avg_line)[minute_bar_length:]
    for line in columns:
        predict_line = res[line]
        predict_line = list(predict_line)[minute_bar_length:]
        ax.fill_between(range(minute_bar_length, 241), avg_line,
                        predict_line, alpha=0.1, color='r')

    return res, ax

res, ax = draw_similarity(target_line, minute_bar_length, similarity_data)
plt.show()