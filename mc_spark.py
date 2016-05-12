# -*- coding: utf-8 -*-
__author__ = 'andy'
"""
    spark 实现蒙特卡罗方法估算pi值
"""

import operator
import numpy as np
from pyspark import SparkContext


# 1.load data
# iterate number
total = 100 * 1000
local_collection = xrange(1, total)

sc = SparkContext("local", "mc_spark")

# parallelize a data set into the cluster
rdd = sc.parallelize(local_collection) \
        .setName("parallelized_data") \
        .cache()

# 2. deal data
# randomly generate points


def map_func(element):
    # print 'map_func: ', element
    x = np.random.random()      # [0, 1)
    y = np.random.random()      # [0, 1)

    return x, y


def map_func_2(element):
    x, y = element
    return 1 if x**2 + y**2 < 1 else 0


rdd2 = rdd.map(map_func)              \
        .setName("random_point")    \
        .cache()

# calculate the number of points in and out the circle

rdd3 = rdd2.map(map_func_2)         \
        .setName("points_in_out_circle")    \
        .cache()

# 3. display the result

in_circle = rdd3.reduce(operator.add)

pi = 4. * in_circle / total

print 'iterator {} times'.format(total)
print 'estimated pi: {}'. format(pi)