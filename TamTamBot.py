# -*- coding: UTF-8 -*-
import json
import logging
import sys
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
        self._debug = None
        self._logging_level = None
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

        self.polling_sleep_time = 5

        self.client = ApiClient(self.conf)

        self.subscriptions = SubscriptionsApi(self.client)
        self.msg = MessagesApi(self.client)
        self.api = BotsApi(self.client)
        self.chats = ChatsApi(self.client)
        self.upload = UploadApi(self.client)

        self.info = self.api.get_my_info()
        if isinstance(self.info, UserWithPhoto):
            self.user_id = self.info.user_id

        self.about = 'Это самый крутой бот в мире, но пока ничего не умеет. Для вызова меню наберите /menu.'
        self.main_menu_title = 'Возможности:'
        self.main_menu_buttons = [
            [CallbackButton('О боте', '/start', Intent.POSITIVE)],
            [CallbackButton('Все чаты бота', '/list_all_chats', Intent.POSITIVE)],
            [LinkButton('Документация по API ТамТам-ботов', 'https://dev.tamtam.chat/')],
            [LinkButton('JSON-схема API ТамТам-ботов', 'https://github.com/tamtam-chat/tamtam-bot-api-schema')],
            [RequestContactButton('Сообщить свои контактные данные')],
            [RequestGeoLocationButton('Сообщить своё местонахождение', True)],
        ]
        self.stop_polling = False

        self.prev_step = {}

        self.debug = False
        self.logging_level = logging.DEBUG if self.debug else logging.INFO

    @property
    def debug(self):
        # type: () -> bool
        return self._debug

    @debug.setter
    def debug(self, val):
        self._debug = val
        self.conf.debug = self._debug

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

    def get_cmd_handler(self, update):
        if not isinstance(update, (Update, UpdateCmn)):
            return False, False
        if not isinstance(update, UpdateCmn):
            update = UpdateCmn(update)
        cmd_handler = 'cmd_handler_%s' % update.cmd
        if hasattr(self, cmd_handler):
            return getattr(self, cmd_handler)

    def call_cmd_handler(self, update):
        handler_exists = False
        if not isinstance(update, (Update, UpdateCmn)):
            return False, False
        if not isinstance(update, UpdateCmn):
            update = UpdateCmn(update)
        handler = self.get_cmd_handler(update)
        if handler:
            handler_exists = True
            # noinspection PyCallingNonCallable
            res = handler(update)
            if res:
                self.prev_step['%s_%s' % (update.chat_id, update.user_id)] = (handler, update)
        else:
            res = False
        return handler_exists, res

    def process_command(self, update):
        # type: (Update) -> bool
        """
        Для обработки команд необходимо создание в наследниках методов с именем "cmd_handler_%s", где %s - имя команды.
        Например, для команды "start" см. ниже метод self.cmd_handler_start
        """
        update = UpdateCmn(update)
        if not update.chat_id:
            return False

        # self.lgz.w('cmd="%s"; user_id=%s' % (cmd, user_id))
        self.lgz.debug('cmd="%s"; chat_id=%s; user_id=%s' % (update.cmd, update.chat_id, update.user_id))
        if update.index in self.prev_step.keys():
            self.prev_step.pop(update.index)
        handler_exists, res = self.call_cmd_handler(update)
        if handler_exists:
            pass
        elif update.cmd == '+':
            res = True
        elif update.cmd == '-':
            res = False
        else:
            self.msg.send_message(NewMessageBody('"%s" - некорректная команда. Пожалуйста, уточните.' % update.cmd, link=update.link), chat_id=update.chat_id)
            res = False
        return res

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

            chats_available = self.get_users_chats_with_bot(update.user_id)
            list_c = []
            for chat_id, chat_ext in chats_available.items():
                chat = chat_ext.chat
                list_c.append('Тип: %s; Название: %s; Участников: %s; Права: %s\n' % (ChatExt.chat_type(chat.type), chat.title, chat.participants_count, chat_ext.admin_permissions.get(self.user_id)))

            if not list_c:
                chs = 'Чатов не найдено.'
            else:
                chs = 'Бот подключен к чатам:\n' + (u'\n'.join(list_c))
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
        while not self.stop_polling:
            # noinspection PyBroadException
            try:
                self.lgz.info('Запрос обновлений')
                ul = self.update_list
                self.lgz.info('Запрос обновлений завершён')
                if ul.updates:
                    self.lgz.info('Имеется %s обновлений' % len(ul.updates))
                    self.lgz.debug(ul)
                    for update in ul.updates:
                        self.lgz.debug(type(update))
                        self.handle_update(update)
                else:
                    self.lgz.info('Событий не было...')
                self.lgz.info('Приостановка на %s секунд' % self.polling_sleep_time)
                sleep(self.polling_sleep_time)

            except ApiException as err:
                if str(err.body).lower().find('Invalid access_token'):
                    raise
            except Exception:
                self.lgz.exception('Exception')
                # raise

    # Обработка тела запроса
    def handle_request_body(self, request_body):
        # type: (bytes) -> None
        if request_body:
            self.lgz.debug('request_body: %s' % request_body)
            data = json.loads(request_body)
            if data.get('update_type'):
                incoming_data = self.client.deserialize(RESTResponse(urllib3.HTTPResponse(request_body)), Update.discriminator_value_class_map.get(data.get('update_type')))
                self.lgz.debug('incoming data:\n type=%s;\n data=%s' % (type(incoming_data), incoming_data))
                if isinstance(incoming_data, Update):
                    self.handle_update(incoming_data)

    def handle_update(self, update):
        # type: (Update) -> bool
        cmd_prefix = '@%s /' % self.info.username
        if isinstance(update, MessageCreatedUpdate) and (update.message.body.text.startswith('/') or update.message.body.text.startswith(cmd_prefix)):
            if update.message.body.text.startswith(cmd_prefix):
                update.message.body.text = str(update.message.body.text).replace(cmd_prefix, '/')
            res = self.process_command(update)
        elif isinstance(update, MessageCreatedUpdate):
            res = self.handle_message_created_update(update)
        elif isinstance(update, MessageCallbackUpdate):
            res = self.handle_message_callback_update(update)
        elif isinstance(update, MessageEditedUpdate):
            res = self.handle_message_edited_update(update)
        elif isinstance(update, MessageRemovedUpdate):
            res = self.handle_message_removed_update(update)
        elif isinstance(update, BotStartedUpdate):
            res = self.handle_bot_started_update(update)
        elif isinstance(update, BotAddedToChatUpdate):
            res = self.handle_bot_added_to_chat_update(update)
        elif isinstance(update, BotRemovedFromChatUpdate):
            res = self.handle_bot_removed_from_chat_update(update)
        elif isinstance(update, UserAddedToChatUpdate):
            res = self.handle_user_added_to_chat_update(update)
        elif isinstance(update, UserRemovedFromChatUpdate):
            res = self.handle_user_removed_from_chat_update(update)
        elif isinstance(update, ChatTitleChangedUpdate):
            res = self.handle_chat_title_changed_update(update)
        else:
            res = False
        return res

    def handle_message_created_update(self, update):
        # type: (MessageCreatedUpdate) -> bool
        update = UpdateCmn(update)
        # Проверка на ответ команде
        if update.index in self.prev_step.keys():
            (handler, update_previous) = self.prev_step[update.index]
            if handler and isinstance(update_previous, UpdateCmn):
                # Если это ответ на вопрос команды, то установить соответствующий признак и снова вызвать команду
                update.is_cmd_response = True
                update.update_previous = update_previous.update_current
                res = handler(update)
                if res:
                    self.prev_step.pop(update.index)
                return res

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
        # type: (int) -> dict
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

    # Формирует список чатов пользователя, в которых он админ, к которым подключен бот с админскими правами
    def get_users_chats_with_bot(self, user_id):
        # type: (int) -> dict
        chats_available = {}
        chat_list = self.chats.get_chats()
        if isinstance(chat_list, ChatList):
            for chat in chat_list.chats:
                self.lgz.debug('Найден чат => chat_id=%s; Тип: %s; Статус: %s; Название: %s; Участников: %s; Владелец: %s' %
                               (chat.chat_id, chat.type, chat.status, chat.title, chat.participants_count, chat.owner_id))
                if chat.status in [ChatStatus.ACTIVE]:
                    members = None
                    try:
                        members = self.get_chat_members(chat.chat_id)
                    except ApiException as err:
                        if not str(err.body).lower().find('User is not admin'):
                            raise
                    if members:
                        chat_ext = ChatExt(chat)
                        chat_ext.admin_permissions[self.user_id] = members.get(self.user_id).permissions
                        current_user = members and members.get(user_id)
                        if current_user and current_user.is_admin:
                            chat_ext.admin_permissions[user_id] = members.get(self.user_id).permissions
                            chats_available[chat.chat_id] = chat_ext
        return chats_available

    def view_buttons(self, title, buttons, user_id=None, chat_id=None, link=None):
        # type: (str, list, int, int, NewMessageLink) -> SendMessageResult
        if buttons:
            mb = self.add_buttons_to_message_body(NewMessageBody(title, link=link), buttons)
        else:
            mb = NewMessageBody('Доступных элементов не найдено.', link=link)
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
            CallbackButtonCmd('Да', cmd_dict['yes']['cmd'], cmd_dict['yes']['cmd_args'], Intent.POSITIVE),
            CallbackButtonCmd('Нет', cmd_dict['no']['cmd'], cmd_dict['no']['cmd_args'], Intent.NEGATIVE),
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
