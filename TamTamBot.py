# -*- coding: UTF-8 -*-
import json
import logging
import os
import re
import sqlite3
import sys
import traceback
from datetime import datetime
from threading import Thread
from time import sleep

import requests
import six
import urllib3

from openapi_client import Configuration, Update, ApiClient, SubscriptionsApi, MessagesApi, BotsApi, ChatsApi, \
    UploadApi, MessageCreatedUpdate, MessageCallbackUpdate, BotStartedUpdate, \
    SendMessageResult, NewMessageBody, LinkButton, Intent, InlineKeyboardAttachmentRequest, \
    InlineKeyboardAttachmentRequestPayload, RequestContactButton, RequestGeoLocationButton, \
    MessageEditedUpdate, ChatMembersList, ChatMember, ChatType, ChatList, ChatStatus, InlineKeyboardAttachment, \
    MessageRemovedUpdate, BotAddedToChatUpdate, BotRemovedFromChatUpdate, \
    UserAddedToChatUpdate, UserRemovedFromChatUpdate, ChatTitleChangedUpdate, NewMessageLink, UploadType, \
    UploadEndpoint, VideoAttachmentRequest, PhotoAttachmentRequest, AudioAttachmentRequest, \
    FileAttachmentRequest, Chat, BotInfo, BotCommand, BotPatch
from openapi_client.rest import ApiException, RESTResponse
from .cls import ChatExt, UpdateCmn, CallbackButtonCmd
from .utils.lng import get_text as _, translation_activate
from .utils.utils import str_to_int


class TamTamBotException(Exception):
    pass


class TamTamBot(object):
    _work_threads_max_count = None
    threads = []

    SERVICE_STR_SEQUENCE = chr(8203) + chr(8203) + chr(8203)

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

        self._languages_dict = None
        self._admins_contacts = None

        self.info = None
        try:
            bp = BotPatch(commands=self.commands, description=self.description)
            self.info = self.api.edit_my_info(bp)
        except ApiException:
            self.lgz.exception('ApiException')
            pass
        if isinstance(self.info, BotInfo):
            self.user_id = self.info.user_id
            self.name = self.info.name
            self.username = self.info.username
            self.title = _('bot @%(username)s (%(name)s)') % {'username': self.username, 'name': self.name}
        else:
            self.user_id = None
            self.name = None
            self.username = None
            self.title = None

        self.stop_polling = False

        self.prev_step_table_name = 'tamtambot_prev_step'
        self.user_prop_table_name = 'tamtambot_user_prop'
        self.db_prepare()

    @property
    def about(self):
        # type: () -> str
        self.lgz.warning('The default about string is used. Maybe is error?')
        return _('This is the coolest bot in the world, but so far can not do anything. To open the menu, type /menu.')

    @property
    def main_menu_title(self):
        # type: () -> str
        return _('Abilities:')

    @property
    def main_menu_buttons(self):
        # type: () -> []
        self.lgz.warning('The default main menu buttons is used. Maybe is error?')
        buttons = [
            [CallbackButtonCmd(_('About bot'), 'start', intent=Intent.POSITIVE)],
            [CallbackButtonCmd(_('All chat bots'), 'list_all_chats', intent=Intent.POSITIVE)],
            [LinkButton(_('API documentation for TamTam-bots'), 'https://dev.tamtam.chat/')],
            [LinkButton(_('JSON Diagram API TamTam Bots'), 'https://github.com/tamtam-chat/tamtam-bot-api-schema')],
            [RequestContactButton(_('Report your contact details'))],
            [RequestGeoLocationButton(_('Report your location'), True)],
        ]
        if len(self.languages_dict) > 1:
            buttons.append([CallbackButtonCmd('Изменить язык / set language', 'set_language', intent=Intent.DEFAULT)])

        return buttons

    @property
    def token(self):
        # type: () -> str
        raise NotImplementedError

    @property
    def description(self):
        # type: () -> str
        raise NotImplementedError

    def get_commands(self):
        # type: () -> [BotCommand]
        self.lgz.warning('The default command list is used. Maybe is error?')
        commands = [
            BotCommand('start', 'начать (о боте) | start (about bot)'),
            BotCommand('menu', 'показать меню | display menu'),
            BotCommand('list_all_chats', 'список всех чатов | list all chats'),
        ]
        if len(self.languages_dict) > 1:
            commands.append(BotCommand('set_language', 'изменить язык | set language'))
        return commands

    @property
    def admins_contacts(self):
        # type: () -> {[]}
        if self._admins_contacts is None:
            self._admins_contacts = {}
            # Формат: chats:-70934954694426,-70968954694437;users:591582322454,123582322123;
            l_fe = os.environ.get('TT_BOT_ADMINS_CONTACTS')
            if l_fe:
                f_users = re.match(r'.*;?users:(-?\d.+?);.*', l_fe)
                f_chats = re.match(r'.*;?chats:(-?\d.+?);.*', l_fe)
                if f_users:
                    l_el = []
                    for _ in f_users.groups()[0].split(','):
                        el = str_to_int(_)
                        if el:
                            if el not in l_el:
                                l_el.append(el)
                    self._admins_contacts['users'] = l_el
                if f_chats:
                    l_el = []
                    for _ in f_chats.groups()[0].split(','):
                        el = str_to_int(_)
                        if el:
                            if el not in l_el:
                                l_el.append(el)
                    self._admins_contacts['chats'] = l_el

        return self._admins_contacts

    @property
    def languages_dict(self):
        if self._languages_dict is None:
            self._languages_dict = {}
            l_fe = os.environ.get('LANGUAGES', 'ru=Русский:en=English')
            l_l = l_fe.split(':')
            for l_c in l_l:
                l_r = l_c.split('=')
                if len(l_r) == 2:
                    self._languages_dict[l_r[0]] = l_r[1]
            if not self._languages_dict:
                self._languages_dict = {'ru': 'Русский', 'en': 'English'}
        return self._languages_dict

    def get_default_language(self):
        return list(self.languages_dict.keys())[0]

    def get_user_language_by_update(self, update):
        # type: (Update) -> str
        language = self.get_default_language()
        update = UpdateCmn(update)
        if update:
            cursor = self.conn_srv.cursor()
            # noinspection SqlResolve
            cursor.execute(
                'SELECT [language] FROM %(table)s WHERE [user_id]=:user_id' %
                {'table': self.user_prop_table_name}, {'user_id': update.user_id})
            row = cursor.fetchone()
            cursor.close()
            if row:
                language = row[0] or self.get_default_language()
                self.lgz.debug(' -> update.user_id=%s -> language: "%s"' % (update.user_id, language))
        return language

    def set_user_language_by_update(self, update, language):
        # type: (Update, str) -> None
        language = language or self.get_default_language()
        update = UpdateCmn(update)
        if update:
            cursor = self.conn_srv.cursor()
            # noinspection SqlResolve
            cursor.execute(
                'SELECT COUNT([language]) FROM %(table)s WHERE [user_id]=:user_id' %
                {'table': self.user_prop_table_name}, {'user_id': update.user_id})
            row = cursor.fetchone()

            if row[0] > 0:
                # noinspection SqlResolve
                cursor.execute(
                    'UPDATE %(table)s SET [language] = :language WHERE [user_id]=:user_id' %
                    {'table': self.user_prop_table_name}, {'language': language, 'user_id': update.user_id})
            else:
                # noinspection SqlResolve
                cursor.execute(
                    'INSERT INTO %(table)s ([user_id], [language]) VALUES (:user_id, :language)' %
                    {'table': self.user_prop_table_name}, {'language': language, 'user_id': update.user_id})
            cursor.connection.commit()
            cursor.close()
            self.lgz.debug(' -> update.user_id=%s -> language: "%s"' % (update.user_id, language))

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

    def set_encoding_for_p2(self, encoding='utf8'):
        if six.PY3:
            return
        else:
            # noinspection PyCompatibility,PyUnresolvedReferences
            reload(sys)
            # noinspection PyUnresolvedReferences
            sys.setdefaultencoding(encoding)
            self.lgz.info('The default encoding is set to %s' % sys.getdefaultencoding())

    @classmethod
    def check_commands(cls, commands):
        # type: ([BotCommand]) -> []
        err_c = []
        for cmd in commands:
            handler_name = 'cmd_handler_%s' % cmd.name
            if hasattr(cls, handler_name):
                cmd_h = getattr(cls, handler_name)
                if not callable(cmd_h):
                    err_c.append(handler_name)
            else:
                err_c.append(handler_name)
        return err_c

    @property
    def commands(self):
        # type: () -> [BotCommand]
        l_c = self.get_commands()
        l_e = self.check_commands(l_c)
        if l_e:
            raise TamTamBotException('Error in command list. Not found handlers :%s.' % l_e)
        else:
            return l_c

    @property
    def conn_srv(self):
        return sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ttb.sqlite3'))

    def db_prepare(self):
        # Создание таблицы
        sql_s = '''
            CREATE TABLE IF NOT EXISTS %s (
                [index]  CHAR (64) PRIMARY KEY,
                [update] TEXT      NOT NULL
            );        
            CREATE TABLE IF NOT EXISTS %s (
                [user_id]  INT     PRIMARY KEY,
                [language] CHAR (10)
            );        
        ''' % (self.prev_step_table_name, self.user_prop_table_name)
        self.conn_srv.cursor().executescript(sql_s)

    @classmethod
    def work_threads_max_count(cls):
        if cls._work_threads_max_count is None:
            cls._work_threads_max_count = str_to_int(os.environ.get('WORK_THREADS_MAX_COUNT'))
            if cls._work_threads_max_count is None:
                cls._work_threads_max_count = 15

        return cls._work_threads_max_count

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

    def get_buttons_for_chats_available(self, user_id, cmd):
        # type: (int, str) -> [[CallbackButtonCmd]]
        buttons = []
        chats_available = self.get_users_chats_with_bot(user_id)
        i = 0
        for chat in sorted(chats_available.values()):
            i += 1
            buttons.append([CallbackButtonCmd('%d. %s' % (i, chat.chat_name), cmd, {'chat_id': chat.chat.chat_id}, Intent.DEFAULT, bot_username=self.username)])
        return buttons

    def view_buttons_for_chats_available(self, title, cmd, user_id):
        # type: (str, str, int) -> SendMessageResult
        return self.view_buttons(title, self.get_buttons_for_chats_available(user_id, cmd), user_id)

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
            elif (res or res is None) and update.is_cmd_response:
                self.prev_step_delete(update.index)
        else:
            res = False
        return handler_exists, res

    def process_command_(self, update, waiting_msg=True):
        # type: (Update, bool) -> bool
        """
        Для обработки команд необходимо создание в наследниках методов с именем "cmd_handler_%s", где %s - имя команды.
        Например, для команды "start" см. ниже метод cmd_handler_start
        """
        res_w_m = None

        update = UpdateCmn(update)
        try:
            if not update.chat_id:
                return False
            if update.cmd_bot and (update.cmd_bot != self.username):
                self.lgz.debug('The command "%(cmd)s" is not applicable to the current bot "%(bot_c)s", but for bot "%(bot)s".' % {'cmd': update.cmd, 'bot_c': self.username, 'bot': update.cmd_bot})
                return False

            cmd = update.cmd
            link = update.link
            chat_id = update.chat_id
            chat_type = update.chat_type

            # self.lgz.w('cmd="%s"; user_id=%s' % (cmd, user_id))
            self.lgz.debug('cmd="%s"; chat_id=%s; user_id=%s' % (update.cmd, update.chat_id, update.user_id))

            if waiting_msg and chat_type == ChatType.DIALOG:
                msg_t = (('{%s} ' % self.title) + _('Wait for process your request (%s)...') % cmd) + self.SERVICE_STR_SEQUENCE
                res_w_m = self.msg.send_message(NewMessageBody(msg_t), chat_id=chat_id)

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
        finally:
            if isinstance(res_w_m, SendMessageResult):
                self.msg.delete_message(res_w_m.message.body.mid)

    def process_command(self, update):
        # type: (Update) -> bool
        return self.process_command_(update)

    def cmd_handler_start(self, update):
        # type: (UpdateCmn) -> bool
        if not (update.chat_type in [ChatType.DIALOG]):
            return False
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            return bool(
                self.msg.send_message(NewMessageBody(self.about, link=update.link), chat_id=update.chat_id)
            )

    def cmd_handler_menu(self, update):
        # type: (UpdateCmn) -> bool
        if not (update.chat_type in [ChatType.DIALOG]):
            return False
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            return bool(
                self.view_main_menu(update)
            )

    # Обработка команды смены языка
    def cmd_handler_set_language(self, update):
        # type: (UpdateCmn) -> bool
        if not (update.chat_type in [ChatType.DIALOG]):
            return False
        if not update.chat_id:
            return False

        if len(self.languages_dict) <= 1:
            return False

        self.lgz.debug('update.chat_id=%s, update.user_id=%s, update.user_name=%s, update.is_cmd_response=%s' % (
            update.chat_id, update.user_id, update.user_name, update.is_cmd_response))

        languages = []

        for k, v in self.languages_dict.items():
            languages.append(CallbackButtonCmd(v, 'set_language', {'lang': k}, Intent.DEFAULT, bot_username=self.username))
        if not update.is_cmd_response:  # Обработка самой команды
            if not isinstance(update.cmd_args, dict):
                buttons = self.get_buttons(languages, 'vertical')
                return bool(
                    self.view_buttons('Выберите язык бота (select bot language):', buttons, chat_id=update.chat_id, link=update.link)
                )
            else:
                lc = update.cmd_args.get('lang') or self.get_default_language()
                self.set_user_language_by_update(update.update_current, lc)
                return bool(
                    self.msg.send_message(NewMessageBody('Установлен язык бота (bot language configured): %s' % self.languages_dict[lc], link=update.link), chat_id=update.chat_id)
                )

    # Выводит список чатов пользователя, в которых он админ, к которым подключен бот с админскими правами
    def cmd_handler_list_all_chats(self, update):
        # type: (UpdateCmn) -> bool
        if not (update.chat_type in [ChatType.DIALOG]):
            return False
        if not update.is_cmd_response:  # Ответ текстом не ожидается
            if not update.chat_id:
                return False
            self.lgz.debug('update.chat_id=%s, update.user_id=%s, update.user_name=%s' % (update.chat_id, update.user_id, update.user_name))

            chats_available = self.get_users_chats_with_bot(update.user_id)
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

    def polling(self):
        self.lgz.info('Start. Press Ctrl-Break for stopping.')
        marker = None
        while not self.stop_polling:
            # noinspection PyBroadException
            try:
                self.before_polling_update_list()
                self.lgz.debug('Update request')
                if marker:
                    ul = self.subscriptions.get_updates(marker=marker, types=Update.update_types)
                else:
                    ul = self.subscriptions.get_updates(types=Update.update_types)
                self.lgz.debug('Update request completed')
                marker = ul.marker
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

    @classmethod
    def update_is_service(cls, update):
        # type: (UpdateCmn) -> bool
        res = False
        if update and update.message and update.message.body and update.message.body.text:
            res = update.message.body.text[-(len(cls.SERVICE_STR_SEQUENCE)):] == cls.SERVICE_STR_SEQUENCE
        return res

    def send_admin_message(self, text, update=None, exception=None, notify=True):
        # type: (str, UpdateCmn, Exception, bool) -> bool
        link = None
        if isinstance(update, UpdateCmn):
            link = update.link
        err = ''
        if exception:
            err = traceback.format_exc()
        res = False
        now = datetime.now()
        text = ('%s(bot @%s): %s' % (now, self.username, (text + err)))[:NewMessageBody.MAX_BODY_LENGTH]
        if self.admins_contacts:
            mb = NewMessageBody(text, link=link, notify=notify)
            if self.admins_contacts.get('chats'):
                for el in self.admins_contacts.get('chats'):
                    try:
                        res_s = self.msg.send_message(mb, chat_id=el)
                        res = res or res_s
                    except Exception as e:
                        self.lgz.exception(e)

            if self.admins_contacts.get('users'):
                for el in self.admins_contacts.get('users'):
                    try:
                        res_s = self.msg.send_message(mb, user_id=el)
                        res = res or res_s
                    except Exception as e:
                        self.lgz.exception(e)

        return res

    def send_error_message(self, update, error=None):
        # type: (UpdateCmn, Exception) -> bool
        if not isinstance(update, UpdateCmn):
            return False

        res = None
        main_info = ('{%s} ' % self.title) + _('Your request (%s) cannot be completed at this time (Maintenance mode etc.). Try again later.') % update.cmd
        chat_type = update.chat_type

        if error:
            self.send_admin_message('error', update, error)

        if not self.update_is_service(update):
            try:
                if update and update.user_id:
                    chat = self.chats.get_chat(update.chat_id)
                    if isinstance(chat, Chat):
                        chat = ChatExt(chat, self.title)
                        if isinstance(chat, ChatExt):
                            res = self.msg.send_message(NewMessageBody(
                                main_info + (' (%s)' % chat.chat_name), link=update.link), user_id=update.user_id)
            except Exception as e:
                self.lgz.exception(e)

            if not res:
                if chat_type == ChatType.DIALOG:
                    try:
                        if update and update.chat_id:
                            res = self.msg.send_message(NewMessageBody(main_info, link=update.link), chat_id=update.chat_id)
                    except Exception as e:
                        self.lgz.exception(e)
                else:
                    self.send_admin_message(str(main_info), update)

        return res

    def deserialize_open_api_object(self, b_obj, response_type):
        # type: (bytes, str) -> object
        incoming_data = self.client.deserialize(urllib3.HTTPResponse(b_obj), response_type)
        return incoming_data

    def serialize_open_api_object(self, obj):
        # type: (object) -> bytes
        return json.dumps(self.client.sanitize_for_serialization(obj))

    def deserialize_update(self, b_obj):
        # type: (bytes) -> Update
        data = json.loads(b_obj)
        incoming_data = None
        if data.get('update_type'):
            incoming_data = self.client.deserialize(RESTResponse(urllib3.HTTPResponse(b_obj)), Update.discriminator_value_class_map.get(data.get('update_type')))
        return incoming_data

    def serialize_update(self, update):
        # type: (Update) -> bytes
        return self.serialize_open_api_object(update)

    # Обработка тела запроса
    def handle_request_body(self, request_body):
        # type: (bytes) -> None
        for t in TamTamBot.threads:
            if not t.is_alive():
                self.lgz.debug('stop %s!' % t)
                t.join()
                TamTamBot.threads.remove(t)
        if len(TamTamBot.threads) < TamTamBot.work_threads_max_count():
            t = Thread(target=self.handle_request_body_, args=(request_body,))
            # noinspection PyBroadException
            try:
                TamTamBot.threads.append(t)
                t.setDaemon(True)
                self.lgz.debug('Thread started. Threads count=%s' % len(TamTamBot.threads))
                t.start()
            except Exception:
                self.lgz.exception('Exception')
            finally:
                self.lgz.debug('exited')
        else:
            err = 'Threads pool is full. The maximum number (%s) is used.' % TamTamBot.work_threads_max_count()
            self.lgz.debug(err)
            incoming_data = self.deserialize_update(request_body)
            if isinstance(incoming_data, Update):
                update = UpdateCmn(incoming_data)
                self.send_error_message(update)
                self.send_admin_message(err, update)

    # Обработка тела запроса
    def handle_request_body_(self, request_body):
        # type: (bytes) -> None
        incoming_data = None
        # noinspection PyBroadException
        try:
            if request_body:
                self.lgz.debug('request body:\n%s\n%s' % (request_body, request_body.decode('utf-8')))
                request_body = self.before_handle_request_body(request_body)
                incoming_data = self.deserialize_update(request_body)
                if incoming_data:
                    incoming_data = self.after_handle_request_body(incoming_data)
                    self.lgz.debug('incoming data:\n type=%s;\n data=%s' % (type(incoming_data), incoming_data))
                    if isinstance(incoming_data, Update):
                        if not self.update_is_service(UpdateCmn(incoming_data)):
                            self.handle_update(incoming_data)
                        else:
                            self.lgz.debug('This update is service - passed')
        except Exception as e:
            self.lgz.exception('Exception')
            if isinstance(incoming_data, Update):
                self.send_error_message(UpdateCmn(incoming_data), e)
        finally:
            self.lgz.debug('Thread exited. Threads count=%s' % len(TamTamBot.threads))

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
        # noinspection PyBroadException
        try:
            self.lgz.debug(' -> %s' % type(update))
            language = self.get_user_language_by_update(update)
            translation_activate(language)
            self.before_handle_update(update)

            is_command = False
            cmd_prefix = '@%s /' % self.info.username
            if isinstance(update, MessageCreatedUpdate):
                if update.message.body.text.startswith(cmd_prefix):
                    is_command = True
                    update.message.body.text = str(update.message.body.text).replace(cmd_prefix, '/')
                elif update.message.body.text.startswith('/'):
                    if update.message.recipient.chat_type == ChatType.DIALOG:
                        is_command = True

            if is_command:
                self.lgz.debug('entry to %s' % self.process_command)
                res = self.process_command(update)
                self.lgz.debug('exit from %s with result=%s' % (self.process_command, res))
            elif isinstance(update, MessageCreatedUpdate):
                if not self.update_is_service(UpdateCmn(update)):
                    self.lgz.debug('entry to %s' % self.handle_message_created_update)
                    res = self.handle_message_created_update(update)
                    self.lgz.debug('exit from %s with result=%s' % (self.handle_message_created_update, res))
                else:
                    res = False
                    self.lgz.debug('This update is service - passed')
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
        except Exception as e:
            self.lgz.exception('Exception')
            update = UpdateCmn(update)
            self.send_error_message(update, e)

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
            update_previous = UpdateCmn(update_previous)
            res_w_m = None
            try:
                if update.chat_type == ChatType.DIALOG:
                    msg_t = (('{%s} ' % self.title) + _('Wait for process your request (%s)...') % update_previous.cmd) + self.SERVICE_STR_SEQUENCE
                    res_w_m = self.msg.send_message(NewMessageBody(msg_t), chat_id=update.chat_id)

                handler_exists, res = self.call_cmd_handler(update)
            finally:
                if isinstance(res_w_m, SendMessageResult):
                    self.msg.delete_message(res_w_m.message.body.mid)
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

    def get_chat_members(self, chat_id, user_ids=None):
        # type: (int, [int]) -> {ChatMember}
        marker = None
        m_dict = {}
        members = []
        while True:
            if user_ids:
                cm = self.chats.get_members(chat_id, user_ids=user_ids)
            elif marker:
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

    # Определяет доступность чата для пользователя
    def chat_is_available(self, chat, user_id):
        # type: (Chat, int) -> ChatExt
        if isinstance(chat, Chat):
            if chat.status in [ChatStatus.ACTIVE]:
                members = None
                bot_user = None
                try:
                    if chat.type != ChatType.DIALOG:
                        bot_user = self.chats.get_membership(chat.chat_id)
                        if isinstance(bot_user, ChatMember):
                            # Только если бот админ
                            if bot_user.is_admin:
                                members = self.get_chat_members(chat.chat_id, [user_id])
                            else:
                                self.lgz.debug('chat => chat_id=%(id)s - pass, because bot not admin' % {'id': chat.chat_id})
                except ApiException as err:
                    if err.status != 403:
                        raise
                if members or chat.type == ChatType.DIALOG:
                    chat_ext = ChatExt(chat, self.title)
                    if members and chat.type != ChatType.DIALOG:
                        current_user = members.get(user_id)
                        if current_user and current_user.is_admin:
                            if bot_user:
                                chat_ext.admin_permissions[self.user_id] = bot_user.permissions
                            else:
                                self.lgz.debug('Pass, because bot with id=%s not found into chat %s members list' % (self.user_id, chat.chat_id))
                    elif chat.type == ChatType.DIALOG:
                        # Вот так интересно вычисляется id диалога бота с пользователем
                        user_dialog_id = self.user_id ^ user_id
                        if chat_ext.chat.chat_id == user_dialog_id:
                            chat_ext.admin_permissions[self.user_id] = ['write', 'read_all_messages']
                        else:
                            self.lgz.debug('Exit, because dialog_id=%s not for user_id=%s' % (chat.chat_id, user_id))
                    if chat_ext.admin_permissions:
                        return chat_ext
                    else:
                        self.lgz.debug('Pass, because for user_id=%s  not admin permissions into chat_id=%s' % (user_id, chat.chat_id))
                else:
                    self.lgz.debug('Pass, because for user_id=%s  not enough permissions into chat_id=%s' % (user_id, chat.chat_id))
            else:
                self.lgz.debug('chat => chat_id=%(id)s - pass, because bot not active' % {'id': chat.chat_id})

    # Формирует список чатов пользователя, в которых админы и он и бот
    def get_users_chats_with_bot(self, user_id):
        # type: (int) -> dict
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
                    chat_ext = self.chat_is_available(chat, user_id)
                    if chat_ext and chat_ext.admin_permissions:
                        chats_available[chat.chat_id] = chat_ext
                        self.lgz.debug('chat => chat_id=%(id)s added into list available chats' % {'id': chat.chat_id})
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
            CallbackButtonCmd(_('Yes'), cmd_dict['yes']['cmd'], cmd_dict['yes']['cmd_args'], Intent.POSITIVE, bot_username=self.username),
            CallbackButtonCmd(_('No'), cmd_dict['no']['cmd'], cmd_dict['no']['cmd_args'], Intent.NEGATIVE, bot_username=self.username),
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

    def upload_content(self, content, upload_type):
        # type: ([], str) -> dict
        upload_ep = self.upload.get_upload_url(type=upload_type)
        if isinstance(upload_ep, UploadEndpoint):
            rdf = requests.post(upload_ep.url, files={'files': ('file', content, 'multipart/form-data')})
            if rdf.status_code == 200:
                return rdf.json()

    def attach_contents(self, items):
        # type: ([(bytes, str)]) -> []
        if not items:
            return
        attachments = []
        for item in items:
            klass = None
            if item[1] == UploadType.VIDEO:
                klass = VideoAttachmentRequest
            elif item[1] == UploadType.PHOTO:
                klass = PhotoAttachmentRequest
            elif item[1] == UploadType.AUDIO:
                klass = AudioAttachmentRequest
            elif item[1] == UploadType.FILE:
                klass = FileAttachmentRequest

            if klass:
                if not isinstance(item[0], dict):
                    upl = self.upload_content(item[0], item[1])
                    if isinstance(upl, dict):
                        attachments.append(klass(upl))
                else:
                    attachments.append(klass(item[0]))
        return attachments

    # noinspection PyIncorrectDocstring
    def send_message(self, mb, max_retry=20, sl_time=1, **kwargs):
        # type: (NewMessageBody, int, int, dict) -> SendMessageResult
        """
        :param NewMessageBody mb: (required)
        :param int user_id: Fill this parameter if you want to send message to user
        :param int chat_id: Fill this if you send message to chat
        :return: SendMessageResult
                 If the method is called asynchronously,
                 returns the request thread.
        """

        rpt = 0
        while rpt < max_retry:
            try:
                rpt += 1
                self.lgz.debug(str(rpt) + ' trying: send message with post')
                res_msg = self.msg.send_message(mb, **kwargs)
                self.lgz.debug(str(rpt) + ' trying: message is sent')
                return res_msg
            except ApiException as e:
                self.lgz.debug('Warning: status:%(status)s; reason:%(reason)s; body:%(body)s' % {'status': e.status, 'reason': e.reason, 'body': e.body})
                if rpt >= max_retry or e.status != 400:
                    raise
                sleep(sl_time)
