#!/usr/bin/python3

import time
import json
import base64
from MessageConverters.MessageConverter import MessageConverter

'''
--------------------- Payload Definition ---------------------

                   [channel_id] [channel_type] [channel_value]
01: battery      -> 0x01         0x75          [1byte ] Unit: %
03: temperature  -> 0x03         0x67          [2bytes] Unit: °C (℉)
04: humidity     -> 0x04         0x68          [1byte ] Unit: %RH
05: PIR          -> 0x05         0x00          [1byte ] Unit: 
06: light_level  -> 0x06         0xCB          [1byte ] Unit: 
07: CO2          -> 0x07         0x7D          [2bytes] Unit: ppm
08: tVOC         -> 0x08         0x7D          [2bytes] Unit: ppb
09: pressure     -> 0x09         0x73          [2bytes] Unit: hPa
------------------------------------------ AM307

0A: HCHO         -> 0x0A         0x7D          [2bytes] Unit: mg/m3
0B: PM2.5        -> 0x0B         0x7D          [2bytes] Unit: ug/m3
0C: PM10         -> 0x0C         0x7D          [2bytes] Unit: ug/m3
0D: O3           -> 0x0D         0x7D          [2bytes] Unit: ppm
0E: beep         -> 0x0E         0x01          [1byte ] Unit: 
------------------------------------------ AM319
'''
class MS_AM3XX(MessageConverter):
    msg_types = {
        0x01: 'battery', # [1byte ] Unit: %
        0x03: 'temperature', # [2bytes] Unit: °C (℉)
        0x04: 'humidity', # [1byte ] Unit: %RH
        0x05: 'PIR', # [1byte ] Unit: 
        0x06: 'light', # [1byte ] Unit: 
        0x07: 'CO2', # [2bytes] Unit: ppm
        0x08: 'tVOC', # [2bytes] Unit: ppb
        0x09: 'pressure', # [2bytes] Unit: hPa
        0x0A: 'HCHO', # [2bytes] Unit: mg/m3
        0x0B: 'PM2_5', # [2bytes] Unit: ug/m3
        0x0C: 'PM10', # [2bytes] Unit: ug/m3
        0x0D: 'O3', # [2bytes] Unit: ppm
        0x0E: 'beep' # [1byte ] Unit: 
        } # noqa

    def __init__(self, devicename=None):
        super().__init__(devicename)

    def parse_battery(self):
        """parse battery payload (0x01)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_battery'. "
                f"expected 1 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) 
        fields['battery'] = value 
        return fields
    
    def parse_temperature(self):
        """parse temperature payload (0x03)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_temperature'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['temperature'] = value / 10
        return fields

    def parse_humidity(self):
        """parse humidity payload (0x04)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_humidity'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['humidity'] = value * 0.5
        return fields

    def parse_PIR(self):
        """parse pir payload  (0x05)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_humidity'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['PIR'] = value
        return fields

    def parse_light(self):
        """parse light payload (0x06)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_light'. "
                f"expected 1 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['light'] = value
        return fields

    def parse_CO2(self):
        """parse co2 payload (0x07)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_co2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['CO2'] = value
        return fields
   
    def parse_tVOC(self):
        """parse tVOC payload (0x08)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog1'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['tVOC'] = value
        return fields    

    def parse_pressure(self):
        """parse pressure payload (0x09)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog1'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['pressure'] = value / 10
        return fields

    def parse_HCHO(self):
        """parse HCHO payload (0x10)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog1'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) | self.payload.pop(0) << 8
        fields['HCHO'] = value / 100
        return fields
  
    def parse_PM2_5(self):
        """parse PM2_5 payload (0x11)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['PM2_5'] = value
        return fields    

    def parse_PM10(self):
        """parse PM10 payload (0x12)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['PM10'] = value
        return fields    

    def parse_O3(self):
        """parse O3 payload (0x18)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) |
            self.payload.pop(0) << 8)
        fields['O3'] = value
        return fields

    def parse_beep(self):
        """parse beep payload (0x19)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_temp2'. "
                f"expected 1 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['beep'] = value
        return fields

    def _hasDownlinkMessage(self):
        return False

    def _getDownlinkMessage(self):
        return None

    def _convert(self, message):
        message = json.loads(message.decode('utf-8'))
        preconverted = message.get('preconverted')
        self.curr_time = int(time.time())
        self.payload = list(bytearray(base64.b64decode(preconverted.get('payload'))))
        try:
            while len(self.payload) > 1:
                # header
                sensortype_bytes = self.payload.pop(0)
                channeltype_bytes = self.payload.pop(0) # not used for now
               
                self.logger.debug("type: {}".format(
                    hex(sensortype_bytes)
                    )
                )
                sensortype = self.msg_types.get(sensortype_bytes, None)
                if sensortype:
                    method_name = "parse_"+sensortype
                    method = getattr(self, method_name, None)
                    if method:
                        sensorvalues = method()
                        self.logger.debug("method: {}, result:{}".format(
                            method_name,
                            sensorvalues
                        ))
                        if sensorvalues:
                            preconverted.update(sensorvalues)
                    else:
                        self.logger.exception(
                            "Method for {} not implemented".format(
                                method_name))
                else:
                    self.logger.error(
                        "Unknown Sensortype: {}".format(sensortype_bytes))
        except Exception:
            self.logger.exception("Error while trying to decode payload..")
        message['preconverted'] = preconverted
        return json.dumps(message).encode('utf-8')
