# -*- coding: UTF-8 -*-
class ChatExt(object):
    def __init__(self, chat):
        self.chat = chat
        self.admin_permissions = {}

    @staticmethod
    def chat_type(key):
        types = {
            "dialog": 'диалог',
            "chat": 'чат',
            "channel": 'канал',
        }
        return types[key]
