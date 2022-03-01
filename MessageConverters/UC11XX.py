#!/usr/bin/python3

from datetime import timezone
from datetime import datetime
from MessageConverters.MessageConverter import MessageConverter


class UC11XX(MessageConverter):
    """Payload decoder fpr Ursalink UC11xx devices"""

    msg_types = {
            0x00: "digital_input",
            0x01: "digital_output",
            0x02: "analog_input",
            0x03: "analog_output"
        }

    def __init__(self, devicename):
        super().__init__(devicename)
        self.payload = None
        self.downlink_message = None
        self.current_ts = None

    def parse_digital_input(self, channel):
        """parse digital_input payload"""
        entry = {}
        if len(self.payload) == 0:
            self.logger.warning("parse message has no content - skipping.")
            return entry
        fields = {}
        # input
        value = self.payload.pop(0)
        fields['digital_in_'+str(channel)] = int(value)
        entry['fields'] = fields
        return entry

    def parse_digital_output(self, channel):
        """parse digital_output payload"""
        entry = {}
        if len(self.payload) == 0:
            self.logger.warning("parse message has no content - skipping.")
            return entry
        fields = {}
        # input
        value = self.payload.pop(0)
        fields['digital_out_'+str(channel)] = int(value)
        entry['fields'] = fields
        return entry

    def parse_analog_input(self, channel):
        """parse analog_input payload"""
        entry = {}
        if len(self.payload) == 0:
            self.logger.warning("parse message has no content - skipping.")
            return entry
        fields = {}
        # input
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['analog_in_act_'+str(channel)] = int(value)/100
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['analog_in_min_'+str(channel)] = int(value)/100
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['analog_in_max_'+str(channel)] = int(value)/100
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['analog_in_avg_'+str(channel)] = int(value)/100
        entry['fields'] = fields
        return entry

    def parse_analog_output(self, channel):
        """parse analog_output payload"""
        entry = {}
        if len(self.payload) == 0:
            self.logger.warning("parse message has no content - skipping.")
            return entry
        fields = {}
        # input
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['analog_out_'+str(channel)] = int(value)/100
        entry['fields'] = fields
        return entry

    def _hasDownlinkMessage(self):
        """True if downlink payload is available"""
        return self.downlink_message is not None

    def _getDownlinkMessage(self):
        """Get prepared message for downlink"""
        return self.downlink_message

    def _convert(self, payload, port):
        '''
        decode payload from device to gwu format
        ldc publish format:
        [
            {
                "measurement": "abc",
                "tags": {
                    "tag_a": "irgendwas",
                    "tag_b": "irgendwasanderes"
                },
                "time": "2021-02-01",
                "fields": {
                    "field_a": "value_a",
                    "field_n": "value_n"
                }
            }
        ]
        '''
        publ_array = []
        self.current_ts = int(datetime.utcnow().replace(
            tzinfo=timezone.utc).timestamp())
        self.payload = payload
        try:
            while len(self.payload) > 0:
                # header
                message_channel = int(self.payload.pop(0))
                message_type_byte = self.payload.pop(0)
                self.logger.debug(
                    "message channel: {}"
                    "message type: {}".format(
                        message_channel,
                        hex(message_type_byte)
                    )
                )
                message_type = self.msg_types.get(message_type_byte, None)
                if message_type:
                    method_name = "parse_" + message_type
                    method = getattr(self, method_name, lambda: None)
                    if method:
                        entry = method(message_channel)
                        if entry:
                            self.logger.debug(
                                "method_name: {}, result:{}".format(
                                    method_name,
                                    entry))
                            publ_array.extend([entry])
                    else:
                        self.logger.exception(
                            "Method for {} nor implemented".format(
                                method_name))
                else:
                    self.logger.exception(
                        "Unknown Message Type: {}".format(message_type_byte))

        except Exception:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
