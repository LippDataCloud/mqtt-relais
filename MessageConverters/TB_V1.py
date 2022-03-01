#!/usr/bin/python3

import time
import base64
import json
from datetime import datetime
from MessageConverters.MessageConverter import MessageConverter


'''
example payload tb v1:

{"ts":1451649600512, "values":{"key1":"value1", "key2":"value2"}}
'''

class TB_V1(MessageConverter):

    def __init__(self):
        super().__init__()

    def _convert(self, message):
        message_json = json.loads(message.decode('utf-8'))
        self.logger.debug(f'before converter json: {json.dumps(message_json, indent=4)}')
        tb_msg = {}
        if message_json.get('preconverted'):
          tb_msg["values"] = message_json.get('preconverted')
          time_rcv = message_json.get('preconverted').get("received_at")
          if time_rcv:
            tb_msg["ts"] = int(datetime.strptime(time_rcv[0:26] + time_rcv[-1],"%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000)
        self.logger.debug(f'after converter json: {json.dumps(tb_msg, indent=4)}')
        return json.dumps(tb_msg).encode('utf-8')
