# -*- coding: UTF-8 -*-
import re

from openapi_client import Chat, ChatType
from ..utils.lng import get_text as _


class ChatExt(object):
    TT_URL_BASE = 'https://tt.me/'

    def __init__(self, chat, this_dialog_name, admin_permissions=None):
        # type: (Chat, str,{int: [str]}) -> None
        self.chat = chat
        self.this_dialog_name = this_dialog_name
        self.admin_permissions = admin_permissions or {}

        self._chat_id = None
        self._lang = None

    @property
    def chat_id(self):
        if self._chat_id is None and self.chat:
            self._chat_id = self.chat.chat_id
        return self._chat_id

    @property
    def title(self):
        link_s = ''
        if self.chat_user_name:
            link_s = ' (@%s)' % self.chat_user_name
        return '%s%s' % (self.chat.title if self.chat.title else '', link_s)

    @property
    def title_ext(self):
        if self.chat.link:
            link_s = ' (%s)' % self.chat.link
        else:
            link_s = ''
        if self.chat_user_name:
            link_s = ' (@%s)' % self.chat_user_name
        return '%s%s' % (self.chat.title if self.chat.title else '', link_s)

    def get_chat_name(self, title):
        # type: (str) -> str
        chat_name = title
        if not chat_name:
            if self.chat.type == ChatType.DIALOG:
                chat_name = self.this_dialog_name or _('current bot (№%s)' % self.chat.chat_id)
            else:
                chat_name = 'unnamed'
        return '%s <%s>' % (self.chat_type(self.chat.type), chat_name)

    @property
    def chat_name(self):
        # type: () -> str
        return self.get_chat_name(self.title)

    @property
    def chat_name_ext(self):
        # type: () -> str
        return self.get_chat_name(self.title_ext)

    @property
    def lang(self):
        # type: () -> str
        if self._lang is None:
            self._lang = ''
            if isinstance(self.chat, Chat):
                if re.findall(r'[а-яА-я]{4,}', '%s\n\n\n%s' % (self.chat.title, self.chat.description)):
                    self._lang = 'ru'
        return self._lang

    @property
    def chat_user_name(self):
        user_name = ''
        if self.chat.link:
            if not self.chat.link.startswith('%sjoin/' % self.TT_URL_BASE):
                user_name = self.chat.link.replace('%s' % self.TT_URL_BASE, '')
        return user_name

    @property
    def public_name(self):
        public_name = None
        if self.chat.link:
            public_name = self.chat.link.replace('%s' % self.TT_URL_BASE, '')
        return public_name

    @staticmethod
    def chat_type(key):
        types = {
            "dialog": _('dialog'),
            "chat": _('chat'),
            "channel": _('channel'),
        }
        return types[key]

    def __eq__(self, other):
        return self.chat_name == other.chat_name

    def __ne__(self, other):
        return self.chat_name != other.chat_name

    def __gt__(self, other):
        return self.chat_name > other.chat_name

    def __lt__(self, other):
        return self.chat_name < other.chat_name

    def __ge__(self, other):
        return self.chat_name >= other.chat_name

    def __le__(self, other):
        return self.chat_name <= other.chat_name

    def __str__(self):
        return '%s: %s' % (self.chat_name, self.chat)
