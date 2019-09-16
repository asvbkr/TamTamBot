# -*- coding: UTF-8 -*-
import json
import re

from TamTamBot.utils.utils import get_param_value, str_to_int
from openapi_client import Update, MessageCallbackUpdate, MessageLinkType, NewMessageLink, BotStartedUpdate, MessageCreatedUpdate, ChatType, User, Message


class UpdateCmn(object):
    def __init__(self, update):
        # type: (Update) -> None

        self.update_current = update
        self.update_type = update.update_type
        self.timestamp = update.timestamp
        self.message = None
        self.cmd_bot = None
        self.cmd = None
        self.cmd_args = None
        self.link = None
        self.user = None
        self.user_id = None
        self.user_name = None
        self.user_id_recipient = None
        self.chat_id = None
        self.chat_type = None
        self.this_cmd_response = False
        self.required_cmd_response = False
        self.update_previous = None
        self.recipient = None

        if isinstance(update, MessageCallbackUpdate):
            self.cmd = update.callback.payload
            self.link = None

            try:
                payload = json.loads(update.callback.payload)
            except json.decoder.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                self.cmd_bot = payload.get('bot')
                self.cmd = payload.get('cmd')
                self.cmd_args = payload.get('cmd_args')
                mid = payload.get('mid')
                if mid:
                    self.link = NewMessageLink(MessageLinkType.REPLY, mid)
            else:  # Для совместимости со старым форматом payload
                cmd = get_param_value(update.callback.payload, 'cmd')
                if cmd:
                    self.cmd = cmd
                mid = get_param_value(update.callback.payload, 'mid')
                if mid:
                    self.link = NewMessageLink(MessageLinkType.REPLY, mid)
                fk = get_param_value(update.callback.payload, 'cmd_args')
                if fk:
                    self.cmd_args = fk
                    chat_id = str_to_int(fk)
                    if chat_id is not None:
                        self.cmd_args = {'chat_id': chat_id}
                    else:
                        self.cmd_args = {'id_str': fk}

            self.chat_id = update.message.recipient.chat_id
            self.user = update.callback.user
            self.user_id = update.callback.user.user_id
            self.user_name = update.callback.user.name
            self.chat_type = update.message.recipient.chat_type
        elif isinstance(update, MessageCreatedUpdate):
            self.cmd = update.message.body.text
            self.link = NewMessageLink(MessageLinkType.REPLY, update.message.body.mid)
            if update.message.recipient:
                self.chat_id = update.message.recipient.chat_id
                self.chat_type = update.message.recipient.chat_type
                self.user_id_recipient = update.message.recipient.user_id
            if update.message.sender:
                self.user = update.message.sender
                self.user_id = update.message.sender.user_id
                self.user_name = update.message.sender.name

            # Обработка аргументов команды типа /get_ids 1 2 7
            # Поддерживается два формата:
            # * update.cmd_arg['l1']['c1'] - строка1, колонка 1
            # * update.cmd_arg['c_parts'] - список строк, каждая из которых содержит список колонок
            # Разделение на строки и колоонки производится по реальным строкам и элементам в строке, разделённых пробелом
            f = re.match(r'(/\w+) (.+)', self.cmd, re.DOTALL)
            if f:
                self.cmd = f.group(1)
                self.cmd_args = self.cmd_args or {}
                i = 1
                for l in f.group(2).split('\n'):
                    if not isinstance(self.cmd_args.get('c_parts'), list):
                        self.cmd_args['c_parts'] = [[]]
                    else:
                        self.cmd_args['c_parts'].append([])

                    ind_l = 'l%s' % i
                    j = 1
                    for c in l.split(' '):
                        if len(c.strip()) > 0:
                            self.cmd_args['c_parts'][-1].append(c)

                        ind_c = 'c%s' % j
                        if not self.cmd_args.get(ind_l):
                            self.cmd_args[ind_l] = {}
                        self.cmd_args[ind_l][ind_c] = c
                        j += 1
                    i += 1

        elif isinstance(update, BotStartedUpdate):
            self.cmd = '/start'
            self.link = None
            self.user_name = None
            self.chat_type = ChatType.DIALOG

        if self.user is None:
            if hasattr(update, 'user'):
                self.user = update.user
            elif hasattr(update, 'sender'):
                self.user = update.message.sender
            if isinstance(self.user, User):
                if self.user_id is None:
                    self.user_id = self.user.user_id
                if self.user_name is None:
                    self.user_name = self.user.name

        if self.chat_id is None:
            if hasattr(update, 'chat_id'):
                self.chat_id = update.chat_id

        if self.user_id is None:
            if hasattr(update, 'user_id'):
                self.user_id = update.user_id

        if hasattr(update, 'message') and isinstance(update.message, Message):
            self.message = update.message
            if hasattr(self.message, 'recipient'):
                self.recipient = self.message.recipient

        if self.cmd:
            self.cmd = self.cmd[1:]

        self._index = None

    @property
    def index(self):
        if not self._index:
            self._index = '%s_%s' % (self.chat_id, self.user_id)
        return self._index
