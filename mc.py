# -*- coding: utf-8 -*-
__author__ = 'andy'

"""
    python 实现蒙特卡罗方法估算pi值
"""

import numpy as np


def mc_pi(n=100):
    """
        Use Monte Calo Method to estimate pi.
        用蒙特卡罗方法估算 pi 值，核心方法是利用正方形和圆形面积的比例：

        首先，我们在坐标轴上构造一个边长为 1 的正方形
        其次，我们以 (0, 0) 为圆心，构造一个半径为 1 的圆形
        此时我们知道这个圆形有 1/4 是在正方形中的，正方形的面积和这 1/4 圆的面积分别是：1 和 pi/4，即 1/4 圆的面积和正方形面积之比刚好是 pi/4
        然后通过蒙特卡罗模拟，看看这个比例大概是多少，模拟方法如下：
        随机扔 n 个点 (x, y)，其中 x, y 都在 0 和 1 之间
        如果 x^2 + y^2 < 1，则把这个点标注为红色，表示这个点落在圆内
        最后数数有 n 个点中有多少点是红点，即落在圆内，假设点数为 m
        则这个 1/4 圆的面积和正方形面积的比例应该是：m/n，即 m/n = pi/4 => pi = 4*m/n

    """

    m = 0
    i = 0
    while i < n:
        x, y = np.random.rand(2)
        if x**2 + y**2 < 1:
            m += 1

        i += 1

    pi = 4. * m / n

    return {"total_point": n, "point_in_circle": m, "estimate_pi": pi}

if __name__ == '__main__':
    print mc_pi(100000000)



