import datetime as dt


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


def datetime_to_int(_dt, date_day=None, date_time=None, time_int=None):
    if date_day:
        return _dt.year * 10000 + _dt.month * 100 + _dt.day
    if date_time:
        return _dt.hour * 10000 + _dt.minute * 100 + _dt.second
    if time_int:
        return _dt.hour * 3600 + _dt.minute * 60 + _dt.second


def datetime_format(
        _dt, repair=1, date_day=None, date_month=None,
        date_time=None, date_hour=None, date_minute=None, format_sisign="-"
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
        return "{Y}{si}{M}{si}{D}".format(Y=_year, M=_month, D=_day, si=format_sisign)
    if date_month:
        return "{Y}{si}{M}".format(Y=_year, M=_month, si=format_sisign)
    if date_time:
        return "{Y}{si}{M}{si}{D} {H}:{m}:{s}".format(
            Y=_year, M=_month, D=_day, H=_hour, m=_minute, s=_second, si=format_sisign
        )
    if date_hour:
        return "{Y}{si}{M}{si}{D} {H}".format(
            Y=_year, M=_month, D=_day, H=_hour, si=format_sisign
        )
    if date_minute:
        return "{Y}{si}{M}{si}{D} {H}:{m}".format(
            Y=_year, M=_month, D=_day, H=_hour, m=_minute, si=format_sisign
        )
    return "--"
