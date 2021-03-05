import datetime as dt
import copy


def fixed_length(_s, length=2, fixed="0", behind=None):
    if not isinstance(_s, str):
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


def datetime_format_code(
        _dt, repair=1, code='{Y}-{M}-{D}'
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
    return code.format(Y=_year, M=_month, D=_day, h=_hour, m=_minute, s=_second)


def date_num_dict(date, days):
    date = dt.datetime.strptime(str(date), "%Y-%m-%d")
    date_dict = {}
    for _ in range(days):
        _day = _ + 1
        _date = date + dt.timedelta(days=_day)
        date_dict.update({datetime_format(_date, date_day=1): str(_day+1)})
    return date_dict


def date_list(s_date, e_date, num=None, format_code=None):
    if isinstance(s_date, str):
        s_date = dt.datetime.strptime(s_date.split(' ')[0], "%Y-%m-%d")
    if isinstance(e_date, str):
        e_date = dt.datetime.strptime(e_date.split(' ')[0], "%Y-%m-%d")
    _list, _ = [], s_date
    if num:
        for _num in range(num):
            _ = s_date + dt.timedelta(days=_num)
            if format_code:
                _dt = datetime_format_code(_, code=format_code)
            else:
                _dt = _
            _list.append(_dt)
        return _list
    while _ <= e_date:
        if format_code:
            _dt = datetime_format_code(_, code=format_code)
        else:
            _dt = _
        _list.append(_dt)
        _ = _ + dt.timedelta(days=1)
    return _list
