# -*- coding: UTF-8 -*-
import inspect
import re
import time
from datetime import datetime


def dt_timestamp(dt):
    return time.mktime(dt.timetuple())


def datetime_from_unix_time(unix_time):
    return datetime.fromtimestamp(float(unix_time / 1000))


def datetime_to_unix_time(dt):
    return int(dt_timestamp(dt) * 1000)


def calling_function_name(level=2):
    st = inspect.stack(0)
    try:
        res_str_num = st[level][2]
    except IndexError:
        res_str_num = None
    try:
        res_name = st[level][3]
    except IndexError:
        res_name = None
    return res_name, res_str_num


def get_param_value(content, parameter):
    # type: (str, str) -> str
    # Получение параметра из строки вида: '{{cmd=/street}}'
    found = re.match('.*({%s=(.+?)}).*' % parameter, content)
    if found:
        return found.group(2)
