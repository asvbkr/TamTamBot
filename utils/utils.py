# -*- coding: UTF-8 -*-
import hashlib
import inspect
import math
import os
import re
import time
from datetime import datetime

import six


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


def get_param_value(content, parameter, ends='{}'):
    # type: (str, str, str) -> str
    # Получение параметра из строки вида: '{{cmd=/street}}'
    content = content or ''
    parameter = parameter or ''
    found = re.match('.*(%s%s=(.+?)%s).*' % ('\\' + ends[0], parameter, '\\' + ends[1]), content)
    if found:
        return found.group(2)


def str_to_int(string, default=None):
    value = default
    try:
        if six.PY3:
            value = int(string)
        else:
            # noinspection PyCompatibility,PyUnresolvedReferences
            value = long(string)
    except TypeError:
        pass
    except ValueError:
        pass
    return value


def int_str_to_bool(string, default=False):
    value = default
    if string is not None:
        int_str = str_to_int(string.strip())
        if string and int_str is not None:
            value = bool(int_str)

    return value


def get_environ_int(np, default=None):
    # type: (str, int) -> int
    s = os.environ.get(np)
    if s is None:
        res = default
    else:
        res = str_to_int(s)
        if res is None:
            res = default
    return res


def get_environ_bool(np, default=None):
    # type: (str, bool) -> bool
    res = default
    s = os.environ.get(np)
    if s:
        s = s.lower()
        if s == 'true':
            res = True
        elif s == 'false':
            res = False
    return res


def get_md5_hash_str(str_):
    # type: (str) -> str
    return hashlib.md5(str(str_).encode('utf-8')).hexdigest()


def put_into_text_storage(text_storage, text, max_length):
    # type: ([], str, int) -> []

    max_length = int(max_length)
    if len(text_storage) == 0:
        text_storage.append('')
    ci = len(text_storage) - 1
    if len(text_storage[ci] + text) <= max_length:
        text = text_storage[ci] + text
        text_storage[ci] = text
    else:
        s_m = []
        p_c = math.ceil(len(text) / max_length)
        for i in range(p_c):
            s_m.append(text[max_length * i:max_length * (i + 1)])
        if not text_storage[ci]:
            text_storage.pop(ci)
        text_storage.extend(s_m)

    return text_storage


class ExtList(list):
    def __init__(self, no_double=False):
        self.no_double = no_double
        super(ExtList, self).__init__()

    def append(self, obj):
        if not self.no_double or not (obj in self):
            super(ExtList, self).append(obj)

    def get(self, index):
        try:
            return self[index]
        except IndexError:
            pass
