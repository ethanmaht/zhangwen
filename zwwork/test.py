import pandas as pd
from pyecharts.charts import Bar3D
import pyecharts.options as opts


cost = pd.read_excel(r"C:\Users\111\Desktop\学习课件111.xlsx")
cost = cost.groupby(["book_name", "admin"]).size().sort_values(ascending=False)
x = []
y = []
data = []
for i in cost.items():
    if i[0][0] not in x:
        x.append(i[0][0])
    if i[0][1] not in y:
        y.append(i[0][1])
    data.append([x.index(i[0][0]), y.index(i[0][1]), i[1]])  # 对应的位置[x.index(i[0][0]),y.index(i[0][1])，对应的值i[1]

bar3d = Bar3D()
bar3d.add("", data,
          xaxis3d_opts=opts.Axis3DOpts(x, type_="category",),
          yaxis3d_opts=opts.Axis3DOpts(y, type_="category",),
          zaxis3d_opts=opts.Axis3DOpts(data, type_="value"))
bar3d.set_global_opts(title_opts=opts.TitleOpts(title="账户投放书籍图"),
                      visualmap_opts=opts.VisualMapOpts(max_=100))
bar3d.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
bar3d.render("bar3d.html")
