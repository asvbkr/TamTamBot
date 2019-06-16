# -*- coding: UTF-8 -*-
import json
import logging
import os
import sqlite3
import sys
from threading import Thread

from django.utils.translation import gettext as _
from time import sleep

import six
import urllib3

from openapi_client import Configuration, Update, ApiClient, SubscriptionsApi, MessagesApi, BotsApi, ChatsApi, UploadApi, MessageCreatedUpdate, MessageCallbackUpdate, BotStartedUpdate, \
    SendMessageResult, NewMessageBody, CallbackButton, LinkButton, Intent, InlineKeyboardAttachmentRequest, InlineKeyboardAttachmentRequestPayload, RequestContactButton, RequestGeoLocationButton, \
    MessageEditedUpdate, UserWithPhoto, ChatMembersList, ChatMember, ChatType, ChatList, ChatStatus, InlineKeyboardAttachment, MessageRemovedUpdate, BotAddedToChatUpdate, BotRemovedFromChatUpdate, \
    UserAddedToChatUpdate, UserRemovedFromChatUpdate, ChatTitleChangedUpdate, NewMessageLink
from openapi_client.rest import ApiException, RESTResponse
from .cls import ChatExt, UpdateCmn, CallbackButtonCmd


class TamTamBotException(Exception):
    pass


class TamTamBot(object):

    def __init__(self):
        # Общие настройки - логирование, кодировка и т.п.

        # noinspection SpellCheckingInspection
        formatter = logging.Formatter('%(asctime)s - %(name)s[%(threadName)s-%(thread)d] - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
        self.lgz = logging.getLogger('%s' % self.__class__.__name__)

        fh = logging.FileHandler("bots_%s.log" % self.__class__.__name__, encoding='UTF-8')
        fh.setFormatter(formatter)
        self.lgz.addHandler(fh)

        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setFormatter(formatter)
        self.lgz.addHandler(sh)

        self.set_encoding_for_p2()

        # Собственные настройки бота
        self.conf = Configuration()
        self.conf.api_key['access_token'] = self.token

        self.trace_requests = True if os.environ.get('TRACE_REQUESTS', 'False').lower() == 'true' else False

        logging_level = os.environ.get('LOGGING_LEVEL', 'INFO')
        # noinspection PyProtectedMember
        logging_level = logging._nameToLevel.get(logging_level)
        if logging_level is None:
            self.logging_level = logging.DEBUG if self.trace_requests else logging.INFO
        else:
            self.logging_level = logging_level

        self.polling_sleep_time = 5

        self.client = ApiClient(self.conf)

        self.subscriptions = SubscriptionsApi(self.client)
        self.msg = MessagesApi(self.client)
        self.api = BotsApi(self.client)
        self.chats = ChatsApi(self.client)
        self.upload = UploadApi(self.client)

        self.info = None
        try:
            self.info = self.api.get_my_info()
        except ApiException:
            self.lgz.exception('ApiException')
            pass
        if isinstance(self.info, UserWithPhoto):
            self.user_id = self.info.user_id
            self.name = self.info.name
            self.username = self.info.username
            self.title = _('bot @%(username)s (%(name)s)') % {'username': self.username, 'name': self.name}
        else:
            self.user_id = None
            self.name = None
            self.username = None
            self.title = None

        self.about = _('This is the coolest bot in the world, but so far can not do anything. To open the menu, type /menu.')
        self.main_menu_title = _('Abilities:')
        self.main_menu_buttons = [
            [CallbackButton(_('About bot'), '/start', Intent.POSITIVE)],
            [CallbackButton(_('All chat bots'), '/list_all_chats', Intent.POSITIVE)],
            [LinkButton(_('API documentation for TamTam-bots'), 'https://dev.tamtam.chat/')],
            [LinkButton(_('JSON Diagram API TamTam Bots'), 'https://github.com/tamtam-chat/tamtam-bot-api-schema')],
            [RequestContactButton(_('Report your contact details'))],
            [RequestGeoLocationButton(_('Report your location'), True)],
        ]
        self.stop_polling = False

        self.prev_step_table_name = 'tamtambot_prev_step'
        self.db_prepare()

    @property
    def trace_requests(self):
        # type: () -> bool
        return self.conf.debug

    @trace_requests.setter
    def trace_requests(self, val):
        self.conf.debug = val

    @property
    def logging_level(self):
        # type: () -> int
        return self._logging_level

    @logging_level.setter
    def logging_level(self, val):
        self._logging_level = val
        self.lgz.setLevel(self._logging_level)

    @property
    def token(self):
        # type: () -> str
        raise NotImplementedError

    def set_encoding_for_p2(self, encoding='utf8'):
        if six.PY3:
            return
        else:
            # noinspection PyCompatibility,PyUnresolvedReferences
            reload(sys)
            # noinspection PyUnresolvedReferences
            sys.setdefaultencoding(encoding)
            self.lgz.info('The default encoding is set to %s' % sys.getdefaultencoding())

    @property
    def conn_srv(self):
        return sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ttb.sqlite3'))

    def db_prepare(self):
        # Создание таблицы
        sql_s = '''
            CREATE TABLE IF NOT EXISTS %s (
                [index]  CHAR (64) PRIMARY KEY,
                [update] TEXT      NOT NULL
            )        
        ''' % self.prev_step_table_name
        self.conn_srv.cursor().execute(sql_s)

    @staticmethod
    def add_buttons_to_message_body(message_body, buttons):
        # type: (NewMessageBody, list) -> NewMessageBody
        prev_attachments = message_body.attachments
        message_body.attachments = [InlineKeyboardAttachmentRequest(InlineKeyboardAttachmentRequestPayload(buttons))]
        if prev_attachments:
            for it in prev_attachments:
                if not isinstance(it, InlineKeyboardAttachment):
                    message_body.attachments.append(it)
        return message_body

    def view_main_menu(self, update):
        # type: (UpdateCmn) -> SendMessageResult
        if update.chat_id:
            return self.msg.send_message(self.add_buttons_to_message_body(NewMessageBody(self.main_menu_title), self.main_menu_buttons), chat_id=update.chat_id)

    def get_buttons_for_chats_available(self, user_id, chat_id, cmd):
        # type: (int, int, str) -> [[CallbackButtonCmd]]
        buttons = []
        chats_available = self.get_users_chats_with_bot(user_id, chat_id)
        for chat in sorted(chats_available.values()):
            buttons.append([CallbackButtonCmd(chat.chat_name, cmd, chat.chat.chat_id, Intent.DEFAULT)])
        return buttons

    def view_buttons_for_chats_available(self, title, cmd, user_id, chat_id):
        # type: (str, str, int, int) -> SendMessageResult
        return self.view_buttons(title, self.get_buttons_for_chats_available(user_id, chat_id, cmd), user_id)

    def get_cmd_handler(self, update):
        if not isinstance(update, (Update, UpdateCmn)):
            return False, False
        if not isinstance(update, UpdateCmn):
            update = UpdateCmn(update)
        cmd_handler = 'cmd_handler_%s' % update.cmd
        if hasattr(self, cmd_handler):
            return getattr(self, cmd_handler)

    def call_cmd_handler(self, update):
        # type: (UpdateCmn or Update) -> (bool, bool)
        handler_exists = False
        if not isinstance(update, (Update, UpdateCmn)):
            return False, False
        if not isinstance(update, UpdateCmn):
            update = UpdateCmn(update)
        if not update.is_cmd_response:
            handler = self.get_cmd_handler(update)
            self.prev_step_delete(update.index)
        else:
            handler = self.get_cmd_handler(update.update_previous)
        if handler:
            handler_exists = True
            self.lgz.debug('Handler exists.')
            self.lgz.debug('Call handler %s.' % handler)
            res = False
            if callable(handler):
                self.lgz.debug('entry to %s.' % handler)
                res = handler(update)
                self.lgz.debug('exit from %s.' % handler)
            else:
                self.lgz.debug('Handler %s not callable.' % handler)
            if res and not update.is_cmd_response:
                self.prev_step_write(update.index, update.update_current)
            elif res and update.is_cmd_response:
                self.prev_step_delete(update.index)
        else:
            res = False
        return handler_exists, res

    def process_command_(self, update, waiting_msg=True):
        # type: (Update, bool) -> bool
        """
        Для обработки команд необходимо создание в наследниках методов с именем "cmd_handler_%s", где %s - имя команды.
        Например, для команды "start" см. ниже метод self.cmd_handler_start
        """
        cmd = None
        link = None
        chat_id = None

        res_w_m = None

        try:
            update = UpdateCmn(update)
            if not update.chat_id:
                return False
            cmd = update.cmd
            link = update.link
            chat_id = update.chat_id

            # self.lgz.w('cmd="%s"; user_id=%s' % (cmd, user_id))
            self.lgz.debug('cmd="%s"; chat_id=%s; user_id=%s' % (update.cmd, update.chat_id, update.user_id))

            if waiting_msg:
                res_w_m = self.msg.send_message(NewMessageBody(_('Wait for process your request (%s)...') % cmd), chat_id=chat_id)

            self.lgz.debug('Trying call handler.')
            handler_exists, res = self.call_cmd_handler(update)
            if handler_exists:
                pass
            elif update.cmd == '+':
                self.lgz.debug('Handle "+".')
                res = True
            elif update.cmd == '-':
                self.lgz.debug('Handle "-".')
                res = False
            else:
                self.lgz.debug('Handler not exists.')
                self.msg.send_message(NewMessageBody(_('"%s" is an incorrect command. Please specify.') % cmd, link=link), chat_id=chat_id)
                res = False
            return res
        except Exception:
            self.msg.send_message(NewMessageBody(_('Your request (%s) cannot be completed at this time. Try again later.') % cmd, link=link), chat_id=chat_id)
            raise
        finally:
            if isinstance(res_w_m, SendMessageResult):
                self.msg.delete_message(res_w_m.message.body.mid)

    def process_command(self, update):
        # type: (Update) -> bool
        return self.process_command_(update)

    def cmd_handler_start(self, update):
        # type: (UpdateCmn) -> bool
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            return bool(
                self.msg.send_message(NewMessageBody(self.about, link=update.link), chat_id=update.chat_id)
            )

    def cmd_handler_menu(self, update):
        # type: (UpdateCmn) -> bool
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            return bool(
                self.view_main_menu(update)
            )

    # Выводит список чатов пользователя, в которых он админ, к которым подключен бот с админскими правами
    def cmd_handler_list_all_chats(self, update):
        # type: (UpdateCmn) -> bool
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            if not (update.chat_type in [ChatType.DIALOG]):
                return False
            if not update.chat_id:
                return False
            self.lgz.debug('update.chat_id=%s, update.user_id=%s, update.user_name=%s' % (update.chat_id, update.user_id, update.user_name))

            chats_available = self.get_users_chats_with_bot(update.user_id, update.chat_id)
            list_c = []
            for chat_ext in sorted(chats_available.values()):
                list_c.append(_('%(chat_name)s: participants: %(participants)s; permissions: %(permissions)s\n') %
                              {'chat_name': chat_ext.chat_name, 'participants': chat_ext.chat.participants_count, 'permissions': chat_ext.admin_permissions.get(self.user_id)})

            if not list_c:
                chs = _('Chats not found.')
            else:
                chs = _('Bot connected to the chat:\n\n') + (u'\n'.join(list_c))
            mb = NewMessageBody(chs, link=update.link)
            return bool(
                self.msg.send_message(mb, user_id=update.user_id)
            )

    @property
    def update_list(self):
        """

        :rtype: UpdateList
        """
        return self.subscriptions.get_updates(types=Update.update_types)

    def polling(self):
        self.lgz.info('Start. Press Ctrl-Break for stopping.')
        while not self.stop_polling:
            # noinspection PyBroadException
            try:
                self.before_polling_update_list()
                self.lgz.debug('Update request')
                ul = self.update_list
                self.lgz.debug('Update request completed')
                if ul.updates:
                    self.after_polling_update_list(True)
                    self.lgz.info('There are %s updates' % len(ul.updates))
                    self.lgz.debug(ul)
                    for update in ul.updates:
                        self.lgz.debug(type(update))
                        self.handle_update(update)
                else:
                    self.after_polling_update_list()
                    self.lgz.debug('No updates...')
                self.lgz.debug('Pause for %s seconds' % self.polling_sleep_time)
                sleep(self.polling_sleep_time)

            except ApiException as err:
                if str(err.body).lower().find('Invalid access_token'):
                    raise
            except Exception:
                self.lgz.exception('Exception')
                # raise
        self.lgz.info('Stopping')

    def before_polling_update_list(self):
        pass

    def after_polling_update_list(self, updated=False):
        # type: (bool) -> None
        pass

    def deserialize_update(self, b_obj):
        # type: (bytes) -> Update
        data = json.loads(b_obj)
        incoming_data = None
        if data.get('update_type'):
            incoming_data = self.client.deserialize(RESTResponse(urllib3.HTTPResponse(b_obj)), Update.discriminator_value_class_map.get(data.get('update_type')))
        return incoming_data

    def serialize_update(self, update):
        # type: (Update) -> bytes
        return json.dumps(self.client.sanitize_for_serialization(update))

    # Обработка тела запроса
    def handle_request_body(self, request_body):
        # type: (bytes) -> None

        t = Thread(target=self.handle_request_body_, args=(request_body,))
        # noinspection PyBroadException
        try:
            t.setDaemon(False)
            self.lgz.debug('Thread started')
            t.start()
        except Exception:
            self.lgz.exception('Exception')
        finally:
            self.lgz.debug('exited')

    # Обработка тела запроса
    def handle_request_body_(self, request_body):
        # type: (bytes) -> None
        try:
            if request_body:
                self.lgz.debug('request body:\n%s\n%s' % (request_body, request_body.decode('utf-8')))
                request_body = self.before_handle_request_body(request_body)
                incoming_data = self.deserialize_update(request_body)
                if incoming_data:
                    incoming_data = self.after_handle_request_body(incoming_data)
                    self.lgz.debug('incoming data:\n type=%s;\n data=%s' % (type(incoming_data), incoming_data))
                    if isinstance(incoming_data, Update):
                        self.handle_update(incoming_data)
        finally:
            self.lgz.debug('exited')

    def before_handle_request_body(self, request_body):
        # type: (bytes) -> bytes
        if self:
            return request_body

    def after_handle_request_body(self, incoming_data):
        # type: (object) -> object
        if self:
            return incoming_data

    def handle_update(self, update):
        # type: (Update) -> bool
        self.lgz.debug(' -> %s' % type(update))
        self.before_handle_update(update)
        cmd_prefix = '@%s /' % self.info.username
        if isinstance(update, MessageCreatedUpdate) and (update.message.body.text.startswith('/') or update.message.body.text.startswith(cmd_prefix)):
            if update.message.body.text.startswith(cmd_prefix):
                update.message.body.text = str(update.message.body.text).replace(cmd_prefix, '/')
            self.lgz.debug('entry to %s' % self.process_command)
            res = self.process_command(update)
            self.lgz.debug('exit from %s with result=%s' % (self.process_command, res))
        elif isinstance(update, MessageCreatedUpdate):
            self.lgz.debug('entry to %s' % self.handle_message_created_update)
            res = self.handle_message_created_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_message_created_update, res))
        elif isinstance(update, MessageCallbackUpdate):
            self.lgz.debug('entry to %s' % self.handle_message_callback_update)
            res = self.handle_message_callback_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_message_callback_update, res))
        elif isinstance(update, MessageEditedUpdate):
            self.lgz.debug('entry to %s' % self.handle_message_edited_update)
            res = self.handle_message_edited_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_message_edited_update, res))
        elif isinstance(update, MessageRemovedUpdate):
            self.lgz.debug('entry to %s' % self.handle_message_removed_update)
            res = self.handle_message_removed_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_message_removed_update, res))
        elif isinstance(update, BotStartedUpdate):
            self.lgz.debug('entry to %s' % self.handle_bot_started_update)
            res = self.handle_bot_started_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_bot_started_update, res))
        elif isinstance(update, BotAddedToChatUpdate):
            self.lgz.debug('entry to %s' % self.handle_bot_added_to_chat_update)
            res = self.handle_bot_added_to_chat_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_bot_added_to_chat_update, res))
        elif isinstance(update, BotRemovedFromChatUpdate):
            self.lgz.debug('entry to %s' % self.handle_bot_removed_from_chat_update)
            res = self.handle_bot_removed_from_chat_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_bot_removed_from_chat_update, res))
        elif isinstance(update, UserAddedToChatUpdate):
            self.lgz.debug('entry to %s' % self.handle_user_added_to_chat_update)
            res = self.handle_user_added_to_chat_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_user_added_to_chat_update, res))
        elif isinstance(update, UserRemovedFromChatUpdate):
            self.lgz.debug('entry to %s' % self.handle_user_removed_from_chat_update)
            res = self.handle_user_removed_from_chat_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_user_removed_from_chat_update, res))
        elif isinstance(update, ChatTitleChangedUpdate):
            self.lgz.debug('entry to %s' % self.handle_chat_title_changed_update)
            res = self.handle_chat_title_changed_update(update)
            self.lgz.debug('exit from %s with result=%s' % (self.handle_chat_title_changed_update, res))
        else:
            res = False
        self.after_handle_update(update)
        return res

    def before_handle_update(self, update):
        # type: (Update) -> None
        pass

    def after_handle_update(self, update):
        # type: (Update) -> None
        pass

    def handle_message_created_update(self, update):
        # type: (MessageCreatedUpdate) -> bool
        update = UpdateCmn(update)
        # Проверка на ответ команде
        update_previous = self.prev_step_get(update.index)
        if isinstance(update_previous, Update):
            self.lgz.debug('Command answer detected (%s).' % update.index)
            # Если это ответ на вопрос команды, то установить соответствующий признак и снова вызвать команду
            update.is_cmd_response = True
            update.update_previous = update_previous
            handler_exists, res = self.call_cmd_handler(update)
            return res
        self.lgz.debug('Trivial message. Not commands answer (%s).' % update.index)

    def handle_message_callback_update(self, update):
        # type: (MessageCallbackUpdate) -> bool
        if update.callback.payload:
            self.lgz.debug('MessageCallbackUpdate:\r\n%s' % update.callback.payload)
            res = self.process_command(update)
            if res:
                self.msg.delete_message(update.message.body.mid)
        else:
            res = self.msg.delete_message(update.message.body.mid)
        return res

    def handle_message_edited_update(self, update):
        # type: (MessageEditedUpdate) -> bool
        pass

    def handle_message_removed_update(self, update):
        # type: (MessageRemovedUpdate) -> bool
        pass

    def handle_bot_started_update(self, update):
        # type: (BotStartedUpdate) -> bool
        return self.process_command(update)

    def handle_bot_added_to_chat_update(self, update):
        # type: (BotAddedToChatUpdate) -> bool
        pass

    def handle_bot_removed_from_chat_update(self, update):
        # type: (BotRemovedFromChatUpdate) -> bool
        pass

    def handle_user_added_to_chat_update(self, update):
        # type: (UserAddedToChatUpdate) -> bool
        pass

    def handle_user_removed_from_chat_update(self, update):
        # type: (UserRemovedFromChatUpdate) -> bool
        pass

    def handle_chat_title_changed_update(self, update):
        # type: (ChatTitleChangedUpdate) -> bool
        pass

    def get_chat_members(self, chat_id):
        # type: (int) -> {ChatMember}
        marker = None
        m_dict = {}
        members = []
        while True:
            if marker:
                cm = self.chats.get_members(chat_id, marker=marker)
            else:
                cm = self.chats.get_members(chat_id)
            if isinstance(cm, ChatMembersList):
                marker = cm.marker
                members.extend(cm.members)
                for c in cm.members:
                    if isinstance(c, ChatMember):
                        m_dict[c.user_id] = c
            if not marker:
                break
        return m_dict

    # Формирует список чатов пользователя, в которых админы и он и бот
    def get_users_chats_with_bot(self, user_id, chat_id):
        # type: (int, int) -> dict
        marker = None
        chats_available = {}
        while True:
            if marker:
                chat_list = self.chats.get_chats(marker=marker)
            else:
                chat_list = self.chats.get_chats()
            if isinstance(chat_list, ChatList):
                marker = chat_list.marker
                for chat in chat_list.chats:
                    self.lgz.debug('Found chat => chat_id=%(id)s; type: %(type)s; status: %(status)s; title: %(title)s; participants: %(participants)s; owner: %(owner)s' %
                                   {'id': chat.chat_id, 'type': chat.type, 'status': chat.status, 'title': chat.title, 'participants': chat.participants_count, 'owner': chat.owner_id})
                    if chat.status in [ChatStatus.ACTIVE]:
                        members = None
                        bot_user = None
                        try:
                            if chat.type != ChatType.DIALOG:
                                bot_user = self.chats.get_membership(chat.chat_id)
                                if isinstance(bot_user, ChatMember):
                                    # Только если бот админ
                                    if bot_user.is_admin:
                                        members = self.get_chat_members(chat.chat_id)
                                    else:
                                        self.lgz.debug('chat => chat_id=%(id)s - exit, because bot not admin' % {'id': chat.chat_id})
                                        continue
                        except ApiException as err:
                            if str(err.body).lower().find('user is not admin') < 0:
                                raise
                        if members or chat.type == ChatType.DIALOG:
                            chat_ext = ChatExt(chat, self.title)
                            if members and chat.type != ChatType.DIALOG:
                                current_user = members.get(user_id)
                                if current_user and current_user.is_admin:
                                    if bot_user:
                                        chat_ext.admin_permissions[self.user_id] = bot_user.permissions
                                    else:
                                        self.lgz.debug('Exit, because bot with id=%s not found into chat %s members list' % (self.user_id, chat.chat_id))
                                        continue
                            elif chat.type == ChatType.DIALOG and chat_ext.chat.chat_id == chat_id:
                                chat_ext.admin_permissions[self.user_id] = ['write', 'read_all_messages']
                            if chat_ext.admin_permissions:
                                chats_available[chat.chat_id] = chat_ext
                                self.lgz.debug('chat => chat_id=%(id)s added into list available chats' % {'id': chat.chat_id})
                    else:
                        self.lgz.debug('chat => chat_id=%(id)s - exit, because bot not active' % {'id': chat.chat_id})
                if not marker:
                    break
        return chats_available

    def view_buttons(self, title, buttons, user_id=None, chat_id=None, link=None):
        # type: (str, list, int, int, NewMessageLink) -> SendMessageResult
        if buttons:
            mb = self.add_buttons_to_message_body(NewMessageBody(title, link=link), buttons)
        else:
            mb = NewMessageBody(_('No available items found.'), link=link)
        if not (user_id or chat_id):
            raise TypeError('user_id or chat_id must be defined.')
        if chat_id:
            return self.msg.send_message(mb, chat_id=chat_id)
        else:
            return self.msg.send_message(mb, user_id=user_id)

    def get_yes_no_buttons(self, cmd_dict):
        # type: ([{}]) -> list
        if not cmd_dict:
            return []
        return self.get_buttons([
            CallbackButtonCmd(_('Yes'), cmd_dict['yes']['cmd'], cmd_dict['yes']['cmd_args'], Intent.POSITIVE),
            CallbackButtonCmd(_('No'), cmd_dict['no']['cmd'], cmd_dict['no']['cmd_args'], Intent.NEGATIVE),
        ])

    @staticmethod
    def get_buttons(cbc, orientation='horizontal'):
        # type: ([CallbackButtonCmd], str) -> list
        if not cbc:
            return []
        orientation = orientation or 'horizontal'
        res = []
        for bt in cbc:
            res.append(bt)
        if orientation == 'horizontal':
            res = [res]
        else:
            res = [[_] for _ in res]

        return res

    def prev_step_write(self, index, update):
        # type: (str, Update) -> None
        if not self.prev_step_exists(index):
            self.lgz.debug('Put index %s into previous step stack.' % index)
            b_obj = self.serialize_update(update)
            cursor = self.conn_srv.cursor()
            # noinspection SqlResolve
            cursor.execute(
                'INSERT INTO %(table)s ([index], [update]) VALUES (:index, :update)' %
                {'table': self.prev_step_table_name}, {'index': index, 'update': b_obj})
            cursor.connection.commit()
            cursor.close()
        self.lgz.debug('previous step stack:\n%s' % self.prev_step_all())

    def prev_step_exists(self, index):
        # type: (str) -> bool
        update = self.prev_step_get(index)
        if update:
            return True
        else:
            return False

    def prev_step_delete(self, index):
        # type: (str) -> None
        if self.prev_step_exists(index):
            self.lgz.debug('Deleting index %s from previous step stack.' % index)
            cursor = self.conn_srv.cursor()
            # noinspection SqlResolve
            cursor.execute(
                'DELETE FROM %(table)s WHERE [index]=:index' %
                {'table': self.prev_step_table_name}, {'index': index})
            cursor.connection.commit()
            cursor.close()
            self.lgz.debug('previous step stack:\n%s' % self.prev_step_all())

    def prev_step_all(self):
        # type: () -> {}
        res = {}
        cursor = self.conn_srv.cursor()
        # noinspection SqlResolve
        cursor.execute(
            'SELECT [index], [update] FROM %(table)s' %
            {'table': self.prev_step_table_name})
        sql_res = cursor.fetchall()
        cursor.close()
        if sql_res is not None:
            for row in sql_res:
                res[row[0]] = self.deserialize_update(row[1])
        return res

    def prev_step_get(self, index):
        # type: (str) -> Update
        cursor = self.conn_srv.cursor()
        # noinspection SqlResolve
        cursor.execute(
            'SELECT [index], [update] FROM %(table)s WHERE [index]=:index' %
            {'table': self.prev_step_table_name}, {'index': index})
        row = cursor.fetchone()
        cursor.close()
        if row:
            return self.deserialize_update(row[1])
