# -*- coding: UTF-8 -*-

import gettext
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

use_django = True

LANG = None


def set_use_django(val):
    global use_django
    use_django = val


def get_lang():
    if use_django:
        from django.utils import translation
        return translation
    else:
        if LANG:
            return LANG
        else:
            return gettext.translation('django', os.path.join(BASE_DIR, 'locale'), languages=['ru'])


def get_text(msg_id):
    return get_lang().gettext(msg_id)


def translation_activate(language):
    if use_django:
        get_lang().activate(language)
    else:
        global LANG
        if language:
            LANG = gettext.translation('django', os.path.join(BASE_DIR, 'locale'), languages=[language])
