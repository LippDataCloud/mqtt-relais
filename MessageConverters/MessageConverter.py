#!/usr/bin/env python3
from abc import ABC, abstractmethod
import logging

class MessageConverter(ABC):

    def __init__(self, devicename=None):
        super().__init__()
        self.devicename = devicename
        self.downlinkMessage = None
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f'message converter initialized. {type(self)}'
            )
        #self.logger = logging.getLogger(type(self).__name__)        

    @abstractmethod
    def _convert(self, payload_bytes: bytes):
        """
        abstract method to decode payload.
        must be implemented in subclass and return decoded payload as
        dict.
        """
    def convert(self, payload_bytes: bytes):
        try:
            self.logger.debug(
                f'message converter - message has type {type(payload_bytes)} / length {len(payload_bytes)}'
            )
            self.logger.debug(
                f"message converter - message before conversion: {payload_bytes.decode(encoding='utf-8')}")
            converted_message = self._convert(payload_bytes)
            self.logger.debug(
                f'message converter - converted message has type {type(converted_message)} / length {len(converted_message)}'
            )
            self.logger.debug(
                f"message converter - message after conversion: {converted_message.decode(encoding='utf-8')}")
            return converted_message
        except Exception:
            self.logger.exception("Error while trying to decode payload..")
            return payload_bytes
        

