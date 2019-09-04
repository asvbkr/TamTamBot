# -*- coding: UTF-8 -*-
import threading
from time import sleep
from typing import Optional, Callable, Any, Iterable, Mapping

from TamTamBot.utils.utils import ExtList
from openapi_client import ActionRequestBody, ChatsApi


class ChatActionRequestRepeater(threading.Thread):

    def __init__(self, chats_api, chat_id):
        # type: (ChatsApi, int) -> None
        super(ChatActionRequestRepeater, self).__init__(daemon=True)

        self.actions = ExtList(True)
        self.stopped = False

        if not isinstance(chats_api, ChatsApi):
            raise ValueError("Invalid value for `chats_api`, must not be `ChatsApi` type")  # noqa: E501
        if chat_id is None:
            raise ValueError("Invalid value for `chat_id`, must not be `None`")  # noqa: E501

        self.chats_api = chats_api
        self.chat_id = chat_id

    def action_switch(self, action_name, on=True):
        if on:
            if action_name not in self.actions:
                self.actions.append(action_name)
        else:
            if action_name in self.actions:
                self.actions.remove(action_name)

    def run(self):
        while not self.stopped:
            for act in self.actions:
                self.chats_api.send_action(self.chat_id, ActionRequestBody(act))
                sleep(5)
