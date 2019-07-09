# -*- coding: UTF-8 -*-
import json

from openapi_client import CallbackButton, Button


class CallbackButtonCmd(CallbackButton):
    def __init__(self, text=None, cmd=None, cmd_args=None, intent=None, mid=None, type_cb='callback', bot_username=None):
        # type: (str, str, dict, str, str, str, str) -> None
        self._cmd = None
        self._cmd_args = None
        self.mid = None

        self.cmd = cmd
        self.cmd_args = cmd_args
        self.mid = mid

        payload = {}
        if bot_username:
            payload['bot'] = bot_username
        payload['cmd'] = '/' + cmd
        if cmd_args or mid:
            if cmd_args:
                payload['cmd_args'] = cmd_args
            if mid:
                payload['mid'] = mid
        super(CallbackButtonCmd, self).__init__(text[:Button.MAX_TEXT_LENGTH], json.dumps(payload), intent, type_cb)

    @property
    def cmd(self):
        # type: () -> str
        return self._cmd

    @cmd.setter
    def cmd(self, cmd):
        # type: (str) -> None
        if cmd is None:
            raise ValueError("Invalid value for `cmd`, must not be `None`")
        # if cmd is not None and len(cmd) > 1024:
        #     raise ValueError("Invalid value for `cmd`, length must be less than or equal to `1024`")
        self._cmd = cmd

    @property
    def cmd_args(self):
        # type: () -> str
        return self._cmd_args

    @cmd_args.setter
    def cmd_args(self, cmd_args):
        # type: (str) -> None
        self._cmd_args = cmd_args

    @property
    def mid(self):
        # type: () -> str
        return self._mid

    @mid.setter
    def mid(self, mid):
        # type: (str) -> None
        self._mid = mid
