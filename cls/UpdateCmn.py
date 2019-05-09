# -*- coding: UTF-8 -*-

from TamTamBot.utils.utils import get_param_value
from openapi_client import Update, MessageCallbackUpdate, MessageLinkType, NewMessageLink, BotStartedUpdate, MessageCreatedUpdate, ChatType


class UpdateCmn(object):
    def __init__(self, update):
        # type: (Update) -> None

        self.update_current = update
        self.update_type = update.update_type
        self.timestamp = update.timestamp
        self.message = None
        self.cmd = None
        self.cmd_args = None
        self.link = None
        self.user_id = None
        self.user_name = None
        self.chat_id = None
        self.chat_type = None
        self.is_cmd_response = False
        self.update_previous = None

        if isinstance(update, MessageCallbackUpdate):
            self.cmd = update.callback.payload
            self.link = None
            cmd = get_param_value(update.callback.payload, 'cmd')
            if cmd:
                self.cmd = cmd
            mid = get_param_value(update.callback.payload, 'mid')
            if mid:
                self.link = NewMessageLink(MessageLinkType.REPLY, mid)
            fk = get_param_value(update.callback.payload, 'cmd_args')
            if fk:
                self.cmd_args = fk
            self.chat_id = update.message.recipient.chat_id
            self.user_id = update.callback.user.user_id
            self.user_name = update.callback.user.name
            self.chat_type = update.message.recipient.chat_type
        elif isinstance(update, MessageCreatedUpdate):
            self.cmd = update.message.body.text
            self.link = NewMessageLink(MessageLinkType.REPLY, update.message.body.mid)
            self.chat_id = update.message.recipient.chat_id
            if update.message.sender:
                self.user_id = update.message.sender.user_id
                self.user_name = update.message.sender.name
            self.chat_type = update.message.recipient.chat_type
        elif isinstance(update, BotStartedUpdate):
            self.cmd = '/start'
            self.link = None
            self.chat_id = update.chat_id
            self.user_id = update.user_id
            self.user_name = None
            self.chat_type = ChatType.DIALOG

        if hasattr(update, 'message'):
            self.message = update.message

        if self.cmd:
            self.cmd = self.cmd[1:]

        self._index = None

    @property
    def index(self):
        if not self._index:
            self._index = '%s_%s' % (self.chat_id, self.user_id)
        return self._index
