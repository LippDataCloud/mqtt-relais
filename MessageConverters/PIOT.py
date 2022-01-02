#!/usr/bin/python3

import time
from datetime import timezone
from datetime import datetime
import redis

from MessageConverters.MessageConverter import MessageConverter
# connect to redis
_redis = redis.StrictRedis(
    charset="utf-8",
    decode_responses=True,
    host='127.0.0.1',
    port=6379)


class PIOT(MessageConverter):
    """Payload decoder fpr PIOT Time Terminals"""

    msg_types = {
            0x00: "status",
            0x01: "badge_event",
            0x04: "time_sync",
            0x07: "ack_not_found"
        }

    def __init__(self, devicename):
        super().__init__(devicename)
        self.payload = None
        self.downlink_message = None
        self.current_ts = None

    def send_downlink(self, msg):
        # downlink has to wait some time because of stupid implementation on device side
        # time.sleep(1.8)
        self.logger.debug(f'downlink msg: {msg}')
        self.downlink_message = msg


    def parse_status(self):
        """parse badge_event payload"""
        entry = {}
        if len(self.payload) == 0:
            self.logger.warning("parse message has no content - skipping.")
            return entry
        fields = {}
        # ignore seq no
        self.payload.pop(0)
        self.payload.pop(0)
        # bat level
        value = self.payload.pop(0)
        fields['batt_level'] = str(value)
        # hw&fw version
        value = self.payload.pop(0)
        fields['hwfw'] = str(value)
        entry['fields'] = fields
        return entry

    def parse_badge_event(self):
        """parse badge_event payload"""
        entry = {}
        fields = {}
        # seq no
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['seq no'] = value
        # validate seq_no
        last_seq_no = 0
        if _redis.exists('{}:seq no'.format(self.devicename)):
            last_seq_no = int(_redis.get('{}:seq no'.format(self.devicename)))
        self.logger.debug(
            'current seq_no is {}, '
            'last seq_no was {}'.format(fields['seq no'], last_seq_no))
        if fields['seq no'] > last_seq_no + 1:
            # seq_no from device is too big, packet was lost
            # inform device about required seq_no
            self.logger.warn(
                'seq_no from device ({}) is too big, '
                'packet was lost, inform device about '
                'current seq_no ({})'.format(fields['seq no'], last_seq_no))
            # prepare downlink with current seq_no
            downlink_list = bytearray()
            downlink_list.append(0xA2)  # type is ACK_MESSAGE
            downlink_list.extend(last_seq_no.to_bytes(2, 'little'))  # seq_no
            self.send_downlink(downlink_list)
            self.payload = []
            return entry
        elif fields['seq no'] < last_seq_no + 1:
            self.logger.warn(
                'seq_no from device is too low, '
                'packet was already received, skip this..')
            # seq_no from device is too low,
            # packet was already received, skip this..
            self.payload = []
            return entry

        # seq_no from device is as expected
        self.logger.info("seq_no from device is as expected")
        _redis.set('{}:seq no'.format(self.devicename), fields['seq no'])
        event_count = 0
        if _redis.exists('{}:badge event count'.format(self.devicename)):
            event_count = int(_redis.get('{}:badge event count'.format(
                self.devicename))) + 1
        _redis.set('{}:badge event count'.format(self.devicename), event_count)
        # sent confirmation every 10th badge event
        if event_count % 10 == 0:
            self.logger.info(
                '10 events received. '
                'inform device about current seq_no ({})'.format(
                    fields['seq no']))
            # prepare downlink with current seq_no
            downlink_list = bytearray()
            downlink_list.append(0xA2)  # type is ACK_MESSAGE
            downlink_list.extend(fields['seq no'].to_bytes(2, 'little'))
            self.send_downlink(downlink_list)

        # time
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['ts'] = value
        entry['fields'] = fields
        tags = {}
        # badge uuid
        value = ''.join(format(x, '02x') for x in self.payload)
        self.payload = []
        tags['uuid'] = value
        entry['tags'] = tags
        return entry

    def parse_time_sync(self):
        """parse time_sync payload"""
        entry = {}
        fields = {}
        # seq no
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['seq no'] = value
        # time
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['ts'] = value
        # timediff
        fields['ts_diff'] = self.current_ts - fields['ts']
        entry['fields'] = fields

        # prepare downlink with time delta
        downlink_list = bytearray()
        if fields['ts_diff'] > 0:
            downlink_list.append(0xA0)  # server time > device time
        else:
            downlink_list.append(0xA1)  # server time < device time
        timedelta = abs(fields['ts_diff'])
        self.logger.info(
            'timedelta is {} '
            '(servertime: {}, '
            'devicetime: {}'.format(
                timedelta,
                self.current_ts,
                fields['ts']))
        downlink_list.extend(timedelta.to_bytes(4, 'little'))  # timedelta
        downlink_list.extend(fields['seq no'].to_bytes(2, 'little'))  # seq_no
        self.send_downlink(downlink_list)
        return entry

    def parse_ack_not_found(self):
        """parse ack_not_found payload"""
        entry = {}
        fields = {}
        # current seq no
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['curr seq no'] = value
        # next seq no
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['next seq no'] = value
        # last ack no
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['last_ack_no'] = value
        # sync last event no
        self.logger.warn(
            'syncing seq no to {}. '
            'current seq no on server was {}'.format(
                fields['curr seq no'],
                fields['last_ack_no']))
        _redis.set('{}:seq no'.format(
            self.devicename), fields['curr seq no'])
        # prepare downlink with current seq_no
        downlink_list = bytearray()
        downlink_list.append(0xA2)  # type is ACK_MESSAGE
        downlink_list.extend(fields['curr seq no'].to_bytes(2, 'little'))
        self.send_downlink(downlink_list)
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
        decode payload from PIOT to gwu format
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
        self.current_ts = int(
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        self.payload = payload
        # set a key
        _redis.set('{}:payload'.format(self.devicename), str(payload))
        try:
            while len(self.payload) > 0:
                # header
                messagetype_byte = self.payload.pop(0)
                self.logger.debug("message type: {}".format(
                    hex(messagetype_byte)
                    )
                )
                messagetype = self.msg_types.get(messagetype_byte, None)
                if messagetype:
                    method_name = "parse_" + messagetype
                    method = getattr(self, method_name, lambda: None)
                    if method:
                        entry = method()
                        if entry:
                            # add common tags and fields
                            if "tags" not in entry:
                                entry["tags"] = {}
                            entry["tags"]["messagetype"] = messagetype
                            entry["tags"]["inout"] = self.devicename.split('_')[-1]
                            entry["tags"]["devicename"] = self.devicename
                            self.logger.info(
                                "uplink type: {}, decoded values:{}".format(
                                    method_name,
                                    entry))
                            publ_array.extend([entry])
                    else:
                        self.logger.exception(
                            "Method for {} nor implemented".format(
                                method_name))
                else:
                    self.logger.exception(
                        "Unknown Message Type: {}".format(messagetype_byte))

        except Exception:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
