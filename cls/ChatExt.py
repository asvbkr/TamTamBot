# -*- coding: UTF-8 -*-
from openapi_client import Chat, ChatType
from ..utils.lng import get_text as _


class ChatExt(object):
    def __init__(self, chat, this_dialog_name, admin_permissions=None):
        # type: (Chat, str,{int: [str]}) -> None
        self.chat = chat
        self.this_dialog_name = this_dialog_name
        self.admin_permissions = admin_permissions or {}

        self._chat_id = None

    @property
    def chat_id(self):
        if self._chat_id is None and self.chat:
            self._chat_id = self.chat.chat_id
        return self._chat_id

    @property
    def title(self):
        if self.chat.link:
            link_s = ' (%s)' % self.chat.link
        else:
            link_s = ''
        return '%s%s' % (self.chat.title if self.chat.title else '', link_s)

    @property
    def chat_name(self):
        # type: () -> str
        chat_name = self.title
        if not chat_name:
            if self.chat.type == ChatType.DIALOG:
                chat_name = self.this_dialog_name or _('current bot (№%s)' % self.chat.chat_id)
            else:
                chat_name = 'unnamed'
        return '%s <%s>' % (self.chat_type(self.chat.type), chat_name)

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
