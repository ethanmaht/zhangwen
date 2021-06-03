import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df
import pylab as pl
import math


def func(val_x, ride, x_shift, y_shift):
    return ride * np.log(val_x + x_shift) + y_shift


x = [1, 14, 30]
x = np.array(x)
num = [0.8, 0.5, 0.3]
y = np.array(num)

popt, pcov = curve_fit(func, x, y)
print(popt)
a = popt[0]
b = popt[1]
c = popt[2]

x_2 = [_ + 1 for _ in range(30)]
pl.plot(x, y, label='Real', color='red')  # 画出实际图像
pl.plot(x_2, func(x_2, a, b, c), label='sim', color='blue')  # 预测图像
pl.show()
