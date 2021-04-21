from pyecharts.charts import Bar, Line, Grid, Pie
from pyecharts import options as opts
from pyecharts.globals import ThemeType


class Chart:

    def my_bar(self, xticks, yticks1, yticks2, ylabel1, ylabel2, title, title_link):
        bar = Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
        bar.add_xaxis(xticks)
        bar.add_yaxis(ylabel1, yticks1)
        bar.add_yaxis(ylabel2, yticks2, itemstyle_opts=opts.ItemStyleOpts(color="gray"))
        bar.set_global_opts(title_opts=opts.TitleOpts(title=title, title_link=title_link),
                            datazoom_opts=opts.DataZoomOpts(is_show=True, type_="slider"),
                            xaxis_opts=opts.AxisOpts(is_show=True),
                            toolbox_opts=opts.ToolboxOpts(),
                            legend_opts=opts.LegendOpts(is_show=True))
        bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        # bar.render("条形图.html")
        return bar

    def my_line(self, xticks, yticks1, yticks2, ylabel1, ylabel2, title, title_link):
        line = Line(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
        line.add_xaxis(xticks)
        line.add_yaxis(ylabel1, yticks1, is_smooth=True, linestyle_opts=opts.LineStyleOpts(width=2))
        line.add_yaxis(ylabel2, yticks2, is_smooth=True, linestyle_opts=opts.LineStyleOpts(width=2))
        line.set_global_opts(title_opts=opts.TitleOpts(title=title, title_link=title_link),
                             datazoom_opts=opts.DataZoomOpts(is_show=True),
                             xaxis_opts=opts.AxisOpts(is_show=True),
                             toolbox_opts=opts.ToolboxOpts(),
                             legend_opts=opts.LegendOpts(is_show=True))
        line.set_series_opts(label_opts=opts.LabelOpts(is_show=False),
                             itemstyle_opts=opts.ItemStyleOpts(border_width=4))
        # line.render("折线图.html")
        return line

    def my_pie(self, x_data, y_data, series_name, title):
        data_pair = [list(i) for i in zip(x_data, y_data)]
        pie = Pie(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
        pie.add(series_name=series_name, data_pair=data_pair, radius="35%", label_opts=opts.LabelOpts(is_show=True))
        # series_name饼图标签，data_pair数据源
        pie.set_global_opts(title_opts=opts.TitleOpts(title=title), legend_opts=opts.LegendOpts(is_show=True),
                            toolbox_opts=opts.ToolboxOpts())
        pie.set_series_opts(label_opts=opts.LabelOpts(is_show=True), tooltip_opts=opts.TooltipOpts(trigger="item"))
        # pie.render("饼图.html")
        return pie


def grid(bar, line, pie):
    a = Grid(init_opts=opts.InitOpts(width="1800px", height="800px"))
    a.add(bar, grid_opts=opts.GridOpts(width='450px', height='200px', pos_top="8%", pos_left="8%"))
    # a.add(line, grid_opts=opts.GridOpts(width='450px', height='200px', pos_top="100", pos_left="58%"))
    a.add(line, grid_opts=opts.GridOpts(width='300px', height='100px', pos_top="500", pos_left="150"))
    a.add(bar, grid_opts=opts.GridOpts(width='100px', height='100px', pos_top="500", pos_left="15px"))

    a.render("render2.html")


x_data = ["渠道a", "渠道b", "渠道c"]
y_data = [500, 100, 900]
series_name = "充值金额"
title = "渠道充值表"
a = Chart()
my_pie = a.my_pie(x_data, y_data, series_name, title)
xticks = ["麻衣", "狂许", "女主播", "战神", "灵异"]
yticks1 = [100, 70, 30, 60, 30]
yticks2 = [20, 50, 90, 55, 60]
ylabel1 = "渠道1"
ylabel2 = "渠道2"
title = "图表"
title_link = "www.baidu.com"

my_bar = a.my_bar(xticks, yticks1, yticks2, ylabel1, ylabel2, title, title_link)
my_line = a.my_line(xticks, yticks1, yticks2, ylabel1, ylabel2, title, title_link)
grid(my_bar, my_line, my_pie)
