import datetime as dt
import time


def fixed_length(_s, length=2, fixed="0", behind=None):
    _s = str(_s)
    _len = length - len(_s)
    if _len > 0:
        _fixed = fixed * _len
        if behind:
            return _s + _fixed
        return _fixed + _s
    return _s


def timestamp_to_datetime(ts_date):
    if isinstance(ts_date, int):
        return dt.datetime.fromtimestamp(ts_date)
    if isinstance(ts_date, str):
        try:
            ts_date = int(ts_date)
        except:
            ts_date = 0
        return dt.datetime.fromtimestamp(ts_date)


def datetime_to_int(_dt, date_day=None, date_time=None, time_int=None):
    if date_day:
        return _dt.year * 10000 + _dt.month * 100 + _dt.day
    if date_time:
        return _dt.hour * 10000 + _dt.minute * 100 + _dt.second
    if time_int:
        return _dt.hour * 3600 + _dt.minute * 60 + _dt.second


def datetime_format(
        _dt, repair=1, date_day=None, date_month=None,
        date_time=None, date_hour=None, date_minute=None, format_sign="-"
):
    _year = _dt.year
    if repair:
        _month = fixed_length(_dt.month, )
        _day = fixed_length(_dt.day, )
        _hour = fixed_length(_dt.hour)
        _minute = fixed_length(_dt.minute)
        _second = fixed_length(_dt.second)
    else:
        _month, _day, _hour, _minute, _second = _dt.month, _dt.day, _dt.hour, _dt.minute, _dt.second
    if date_day:
        return "{Y}{si}{M}{si}{D}".format(Y=_year, M=_month, D=_day, si=format_sign)
    if date_month:
        return "{Y}{si}{M}".format(Y=_year, M=_month, si=format_sign)
    if date_time:
        return "{Y}{si}{M}{si}{D} {H}:{m}:{s}".format(
            Y=_year, M=_month, D=_day, H=_hour, m=_minute, s=_second, si=format_sign
        )
    if date_hour:
        return "{Y}{si}{M}{si}{D} {H}".format(
            Y=_year, M=_month, D=_day, H=_hour, si=format_sign
        )
    if date_minute:
        return "{Y}{si}{M}{si}{D} {H}:{m}".format(
            Y=_year, M=_month, D=_day, H=_hour, m=_minute, si=format_sign
        )
    return "--"


def datetime_format_code(_dt, repair=1, code='{Y}-{M}-{D}'):
    if not code:
        return _dt
    if isinstance(_dt, str):
        _dt = dt.datetime.strptime(_dt, "%Y-%m-%d")
    _year = _dt.year
    if repair:
        _month = fixed_length(_dt.month, )
        _day = fixed_length(_dt.day, )
    else:
        _month, _day = _dt.month, _dt.day
    wd, mw, nmw = _dt.weekday(), month_week(_dt), month_week(_dt, natural=1)
    yw, nyw = year_week(_dt), year_week(_dt, natural=1)
    return code.format(
        Y=_year, M=_month, D=_day, wd=wd, mw=mw, nmw=nmw, yw=yw, nyw=nyw
    )


def date_num_dict(date, days):
    date = dt.datetime.strptime(str(date), "%Y-%m-%d")
    date_dict = {}
    for _ in range(days):
        _day = _ + 1
        _date = date + dt.timedelta(days=_day)
        date_dict.update({datetime_format_code(_date, code='{Y}-{M}-{D}'): str(_day+1)})
    return date_dict


def date_list(s_date, e_date=None, num=None, format_code=None, direction=0):
    if isinstance(s_date, str):
        s_date = dt.datetime.strptime(s_date.split(' ')[0], "%Y-%m-%d")
    if isinstance(e_date, str):
        e_date = dt.datetime.strptime(e_date.split(' ')[0], "%Y-%m-%d")
    _list, _ = [], s_date
    if num:
        if isinstance(num, list):
            num = num
        if isinstance(num, int):
            num = range(abs(num))
        if direction:
            for _num in num:
                _ = s_date + dt.timedelta(days=_num - 1)
                _dt = datetime_format_code(_, code=format_code)
                _list.append(_dt)
            return _list
        for _num in num:
            _ = s_date - dt.timedelta(days=_num-1)
            _dt = datetime_format_code(_, code=format_code)
            _list.append(_dt)
        return _list
    if e_date:
        while _ <= e_date:
            _dt = datetime_format_code(_, code=format_code)
            _list.append(_dt)
            _ = _ + dt.timedelta(days=1)
        return _list
    return [s_date]


def year_week(_dt, natural=0):
    year_s = dt.datetime(_dt.year, 1, 1)
    year_e = dt.datetime(_dt.year, _dt.month, _dt.day)
    _day = (year_e - year_s).days
    if natural:
        return _day // 7 + 1
    year_s_wd = dt.datetime(_dt.year, 1, 1).weekday()
    return (_day - year_s_wd) // 7 + 1


def month_week(_dt, natural=0):
    month_s = dt.datetime(_dt.year, _dt.month, 1)
    month_e = dt.datetime(_dt.year, _dt.month, _dt.day)
    _day = (month_e - month_s).days
    if natural:
        return _day // 7 + 1
    month_s_wd = dt.datetime(_dt.year, _dt.month, 1).weekday()
    return (_day - month_s_wd) // 7 + 1


def sub_date(s_date, e_date, types='day') -> int:
    if isinstance(s_date, str):
        s_date = dt.datetime.strptime(s_date, '%Y-%m-%d')
    if isinstance(e_date, str):
        e_date = dt.datetime.strptime(e_date, '%Y-%m-%d')
    try:
        sub = e_date - s_date
        return sub.days
    except TypeError or Exception as b:
        return -1


def date_sub_days(sub_days, _s_day=None):
    if isinstance(_s_day, str):
        _s_day = dt.datetime.strptime(_s_day, '%Y-%m-%d')
    if not _s_day:
        _s_day = dt.datetime.now()
    e_day = _s_day - dt.timedelta(days=sub_days)
    return datetime_format_code(e_day)


def date_sub_days_stamp(sub_days, _s_day=None):
    if isinstance(_s_day, str):
        _s_day = int(_s_day)
    _s_day = _s_day - (sub_days * 24 * 60 * 60)
    return _s_day


def get_last_date_in_unit(_date, unit='month'):
    dt_date = dt.datetime.strptime(_date, '%Y-%m-%d')
    _y, _m = dt_date.year, dt_date.month
    if unit == 'year':
        _name = '_Y' + str(_y)[-2:]
        return _name, '{Y}-01-01'.format(Y=_y + 1)
    if unit == 'month':
        _name = '_M' + str(_y)[-2:] + fixed_length(_m)
        if _m == 12:
            return _name, '{Y}-01-01'.format(Y=_y + 1)
        else:
            return _name, '{Y}-{M}-01'.format(Y=_y, M=_m + 1)
    if unit == 'quarter':
        if _m > 9:
            _name = '_{Y}Q4'.format(Y=_y)
            return _name, '{Y}-01-01'.format(Y=_y + 1)
        if _m > 6:
            _name = '_{Y}Q3'.format(Y=_y)
            return _name, '{Y}-10-01'.format(Y=_y)
        if _m > 3:
            _name = '_{Y}Q2'.format(Y=_y)
            return _name, '{Y}-07-01'.format(Y=_y)
        _name = '_{Y}Q1'.format(Y=_y)
        return _name, '{Y}-04-01'.format(Y=_y)


def block_date_list(date, end_date=None, date_unit='quarter', num=None):
    date = ensure_date_type_is_date(date)
    if end_date:
        end_date = datetime_format_code(end_date)
    else:
        end_date = datetime_format_code(dt.datetime.now())
    date_block_list = []
    while date <= end_date:
        date_block_name, e_date = get_last_date_in_unit(date, date_unit)
        date_block_list.append({'date_name': date_block_name, 's_date': date, 'e_date': e_date})
        date = e_date
    return date_block_list


def date_to_stamp(date):
    if not isinstance(date, str):
        date = str(date)
    date_time = dt.datetime.strptime(date, '%Y-%m-%d')
    un_time = time.mktime(date_time.timetuple())
    return int(un_time)


def ensure_date_type_is_date(_date, format_code=None):
    if isinstance(_date, int):
        time_local = time.strftime("%Y-%m-%d", time.localtime(_date))
        return datetime_format_code(time_local, code=format_code)
    if isinstance(_date, str):
        if '-' not in _date:
            time_local = time.strftime("%Y-%m-%d", time.localtime(int(_date)))
            return datetime_format_code(time_local, code=format_code)
        return _date


def ensure_date_type_is_stamp(_date):
    if isinstance(_date, int):
        return _date
    return date_to_stamp(_date)


class FunctionTimer:
    def __init__(self):
        self.time_start = dt.datetime.now()

    def time_start(self):
        self.time_start = dt.datetime.now()
        print('start time is:', self.time_start)

    def time_spot(self):
        _spot = dt.datetime.now()
        user_time = _spot - self.time_start
        print('has use time:', user_time.seconds, 'seconds')
