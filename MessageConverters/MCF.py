#!/usr/bin/python3

from MessageConverters.MessageConverter import MessageConverter
import logging
from datetime import datetime
from datetime import timezone


class MCF(MessageConverter):
    msg_types = {
            0x01: "time_sync_request",
            0x04: "t_p_rh",
            0x05: "uart",
            0x09: "power",
            0x0A: "io",
            0x0B: "report_data",
            0x0C: "t_p_rh_lux_voc",
            0x0D: "analog_data",
            0x0E: "t_p_rh_lux_voc_co2",
            0x0F: "special_data",
            0x10: "digital_data",
            0x11: "length_error"
        }

    def __init__(self, devicename=None):
        super().__init__(devicename)

    def __parse_time(self):
        a = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        value = list(a.to_bytes(4, byteorder='little'))
        self.logger.info(f"time payload: {value}")
        year = 2000 + (value[3] >> 1)
        month = ((value[3] & 0x01) << 3) | (value[2] >> 5)
        day = value[2] & 0x1f
        hours = value[1] >> 3
        minutes = ((value[1] & 0x7) << 3) | (value[0] >> 5)
        seconds = value[0] & 0x1f
        self.logger.info(
            f'year : {year}, '
            f'month : {month}, '
            f'day : {day}, '
            f'hours : {hours}, '
            f'minutes : {minutes}, '
            f'seconds : {seconds}')
        # datetime(year, month, day, hour, minute, second, microsecond)
        date_time_obj = datetime(year, month, day, hours, minutes, seconds)

        return int(datetime.timestamp(date_time_obj))

    def parse_time_sync_request(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # sync id
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['sync_id'] = value
        # sync version
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16)
        fields['sync version'] = value
        # application type
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields["app_type"] = value
        # option
        value = self.payload.pop(0)
        fields["option"] = value
        entry['fields'] = fields
        return entry

    def parse_t_p_rh(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # time
        fields['time'] = self.__parse_time()
        # temperature
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8) / 100
        fields['temperature'] = value
        # humidity
        value = self.payload.pop(0) / 2
        fields["humidity"] = value
        # pressure1
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16)
        fields["pressure"] = value
        entry['fields'] = fields
        return entry

    def parse_uart(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        self.logger.warn("parse function not implemented - skipping.")
        return entry

    def parse_power(self):
        # TODO: there is a 2nd payload version woth more data..
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # time
        fields['time'] = self.__parse_time()
        # active energy
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['active_energy'] = value
        # active energy
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['active_energy'] = value
        # reactive energy
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['reactive_energy'] = value
        # apparent energy
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['apparent_energy'] = value
        # running_time
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['running_time'] = value
        entry['fields'] = fields
        return entry

    def parse_io(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # time
        fields['time'] = self.__parse_time()
        # inputs
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['inputs'] = bin(value)[2:]
        # outputs
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['outputs'] = bin(value)[2:]
        # events
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 24)
        fields['events'] = bin(value)[2:]
        entry['fields'] = fields
        return entry

    def parse_report_data(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        self.logger.warn("parse function not implemented - skipping.")
        return entry

    def parse_t_p_rh_lux_voc(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # first part is identical with t_p_rh
        fields.update(self.parse_t_p_rh()['fields'])
        # illuminance
        value = value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['illuminance'] = value
        # voc
        value = value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['voc'] = value
        entry['fields'] = fields
        return entry

    def parse_analog_data(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        self.logger.warn("parse function not implemented - skipping.")
        return entry

    def parse_t_p_rh_lux_voc_co2(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # first part is identical with t_p_rh
        fields.update(self.parse_t_p_rh_lux_voc()['fields'])
        # co2
        value = value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['co2'] = value
        entry['fields'] = fields
        return entry

    def parse_special_data(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        self.logger.warn("parse function not implemented - skipping.")
        return entry

    def parse_digital_data(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # type
        value = self.payload.pop(0)
        fields['type'] = value
        if value == 0:
            for num in range(16):
                if (self.payload):
                    value = value = (
                        self.payload.pop(0) |
                        self.payload.pop(0) << 8)
                    fields[f'input_{num}'] = value
        elif (value == 1):
            # time
            fields['time'] = self.__parse_time()
            # frequency
            value = (
                self.payload.pop(0) |
                self.payload.pop(0) << 8)
            fields['frequency'] = value/10
            # battery pecentage (optional)
            if (self.payload):
                value = self.payload.pop(0)
                fields['battery_percentage'] = value          
        elif (value == 2):
            for num in range(5):
                if (self.payload):
                    fields[f'time_{num}'] = self.__parse_time()
                    value = value = (
                        self.payload.pop(0) |
                        self.payload.pop(0) << 8)
                    fields[f'input_{num}'] = value
            # battery pecentage (optional)
            if (self.payload):
                value = self.payload.pop(0)
                fields['battery_percentage'] = value
        else:
            self.logger.warn(f'unknown type "{value}" - skipping.')
        entry['fields'] = fields
        return entry

    def parse_serial_data(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        self.logger.warn("parse function not implemented - skipping.")
        return entry

    def parse_length_error(self):
        entry = {}
        if (len(self.payload) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        # ignore seq no
        self.payload.pop(0) | self.payload.pop(0) << 8
        # bat level
        value = self.payload.pop(0)
        fields['batt_level'] = str(value)
        # hw&fw version
        value = self.payload.pop(0)
        fields['hwfw'] = str(value)
        entry['fields'] = fields
        return entry

    def _hasDownlinkMessage(self):
        return self.downlinkMessage is not None

    def _getDownlinkMessage(self):
        return self.downlinkMessage

    def _convert(self, payload, port):
        '''
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
        dt = datetime.utcnow()
        self.current_ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
        self.current_time = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.payload = list(bytearray(payload))
        self.logger.debug(
            "decoding payload {}. servertime is {} (ts: {})".format(
                    payload,
                    self.current_time,
                    self.current_ts))
        try:
            self.cursor = 0
            while len(self.payload) > 0:
                # header
                messagetype_byte = self.payload.pop(0)
                self.logger.debug("message type: {}".format(
                    hex(messagetype_byte)))
                messagetype = self.msg_types.get(messagetype_byte, None)
                if messagetype:
                    method_name = "parse_" + messagetype
                    method = getattr(self, method_name, lambda: None)
                    if method:
                        entry = method()
                        self.payload = []
                        if entry:
                            # add common tags and fields
                            entry["ts"] = self.current_time
                            if "tags" not in entry:
                                entry["tags"] = {}
                            entry["tags"]["devicename"] = self.devicename
                            entry["tags"]["messagetype"] = messagetype
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
                        "Unknown Message Type: {}".format(messagetype_byte))

        except Exception:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
