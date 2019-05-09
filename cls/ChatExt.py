# -*- coding: UTF-8 -*-
from openapi_client import Chat


class ChatExt(object):
    def __init__(self, chat):
        # type: (Chat) -> None
        self.chat = chat
        self.admin_permissions = {}

    @property
    def title(self):
        if self.chat.link:
            link_s = ' (%s)' % self.chat.link
        else:
            link_s = ''
        return '%s%s' % (self.chat.title, link_s)

    @staticmethod
    def chat_type(key):
        types = {
            "dialog": 'диалог',
            "chat": 'чат',
            "channel": 'канал',
        }
        return types[key]
