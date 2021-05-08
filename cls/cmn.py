# -*- coding: UTF-8 -*-
import re


class TtUtils:
    @staticmethod
    def get_param_value(content, parameter, ends='{}'):
        # type: (str, str, str) -> str
        # Получение параметра из строки вида: '{{cmd=/street}}'
        content = content or ''
        parameter = parameter or ''
        found = re.match('.*(%s%s=(.+?)%s).*' % ('\\' + ends[0], parameter, '\\' + ends[1]), content)
        if found:
            return found.group(2)
