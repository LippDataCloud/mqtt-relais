#!/usr/bin/python3

import time
import json
import base64
from MessageConverters.MessageConverter import MessageConverter


class ELSYS(MessageConverter):
    msg_types = {
            0x00: "reserved",
            0x01: "temperature",  # 2bytes: -3276.5°C-->3276.5°C (Value of: 100=10.0°C) # noqa
            0x02: "humidity",  # 1byte: 0-100%
            0x03: "acceleration",  # 3bytes: X,Y,Z -127-127 (Value of:63=1G)
            0x04: "light",  # 2bytes 0-65535	Lux
            0x05: "motion",  # 1byte: 0-255 (Number of motion count from PIR)
            0x06: "co2",  # 2bytes: 0-10000ppm
            0x07: "battery",  # 2bytes: 0-65535mV
            0x08: "analog1",  # 2bytes: 0-65535mV
            0x09: "gps",  # 6 3bytes: lat,	3bytes long, binary
            0x0A: "pulse_count1",  # 2bytes 0-65535 (between two send intervals)
            0x0B: "pulse_count1_abs",  # 4bytes: Absolute	value		0-4294967295
            0x0C: "external_temp1",  # 2bytes: -3276.5C-->3276.5C
            0x0D: "external_digital",  # 1byte: 0,1	(on/off,	down/up)
            0x0E: "external_distance",  # 2bytes 0-65535mm
            0x0F: "motion_am",  # 1bytes 0-255 (interrupts from accelerometer )
            0x10: "external_ir",  # 4bytes: 2bytes internal temp, 2 bytes	external, -3276.5C-->3276.5C # noqa
            0x11: "occupancy",  # 1byte: 0-255 (0-->nobody,1-->body,2-->Body)
                               # ERSDesk: 0-->no	body,1-->Pending(entering,leaving),2-->Occupied # noqa
                               # ERS Eye: 0-->nobody, 1-->PIR triggered, 2-->Heat triggered # noqa
            0x12: "external_water_leak",  # 1byte 0-255
            0x13: "grideye",  # 65bytes: 1byte ref,64byte pixel temp 8x8 (reserved	for	future	use) # noqa
            0x14: "pressure",  # 4bytes Pressure	data	(hPa)
            0x15: "sound",  # 2bytes: Sound data,1 byte peak, 1byte avg (dB)
            0x16: "pulse_count2",  # 2bytes: 0-65535
            0x17: "pulse_count2_abs",  # 4bytes: Absolute value 0-4294967295
            0x18: "analog2",  # 2bytes: 0-65535mV
            0x19: "external_temp2",  # 2bytes: -3276.5C-->3276.5°C(Value	of:	100=10.0°C) # noqa
            0x1A: "external_digital2",  # 1byte: 0,1(on/off,	down/up)
            0x1B: "external_analog",  # 4bytes: signed int (uV). Analog from ADC-Module # noqa
            0x3D: "debug",  # 4bytes: Data	depends	on	debug	information
            0x3E: "settings"  # n bytes: sent to server at startup (first package). # noqa
                                    # Sent on Port+1. See sensor settings for more information # noqa
        } # noqa

    def __init__(self, devicename=None):
        super().__init__(devicename)

    def parse_temperature(self):
        """parse temperature payload (0x01)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_temperature'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['temperature'] = value / 10
        return fields

    def parse_humidity(self):
        """parse humidity payload (0x02)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_humidity'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['humidity'] = value
        return fields

    def parse_acceleration(self):
        """parse acceleration payload  (0x03)"""
        fields = {}
        if len(self.payload) < 3:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_acceleration'. "
                f"expected 3 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)-62
        fields['acceleration_x'] = value
        value = self.payload.pop(0)-62
        fields['acceleration_y'] = value
        value = self.payload.pop(0)-62
        fields['acceleration_z'] = value
        return fields

    def parse_light(self):
        """parse light payload (0x04)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_light'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['light'] = value
        return fields

    def parse_motion(self):
        """parse motion payload (0x05)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_motion'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
        value = self.payload.pop(0)
        fields['motion'] = value
        return fields

    def parse_co2(self):
        """parse co2 payload (0x06)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_co2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['co2'] = value
        return fields

    def parse_battery(self):
        """parse battery payload (0x07)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_battery'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['battery'] = value / 1000
        return fields
    
    def parse_analog1(self):
        """parse analog1 payload (0x08)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog1'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['analog1'] = value / 1000
        return fields    

    def parse_gps(self):
        """parse gps payload (0x09)"""
        fields = {}
        if len(self.payload) < 6:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_gps'. "
                f"expected 6 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['gps_lat'] = value
        value = (
             self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['gps_long'] = value
        return fields

    def parse_pulse_count1(self):
        """parse pulse_count payload (0x0A)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['pulse_count'] = value
        return fields    

    def parse_pulse_count1_abs(self):
        """parse pulse_count_abs payload (0x0B)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count_abs'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 24 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['pulse_count_abs'] = value
        return fields    

    def parse_external_temp1(self):
        """parse external_temp1 payload (0x0C)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_temp1'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_temp1'] = value / 10
        return fields
    
    def parse_external_digital(self):
        """parse external_digital payload (0x0D)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_digital'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['external_digital'] = value
        return fields
    
    def parse_external_distance(self):
        """parse external_distance payload (0x0E)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_distance'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_distance'] = value / 1000
        return fields

    def parse_motion_am(self):
        """parse motion_am payload (0x0F)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_motion_am'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0)
        fields['motion_am'] = value
        return fields

    def parse_external_ir(self):
        """parse external_distance payload (0x10)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_ir'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_ir_int'] = value / 10
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_ir_ext'] = value / 10
        return fields
    
    def parse_occupancy(self):
        """parse occupancy payload (0x11)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_occupancy'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop
        fields['occupancy'] = value
        return fields

    def parse_external_water_leak(self):
        """parse external_water_leak payload (0x12)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_water_leak'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop
        fields['external_water_leak'] = value
        return fields

    def parse_grideye(self):
        """parse grideye payload (0x13)"""
        fields = {}
        if len(self.payload) < 65:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_grideye'. "
                f"expected 65 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) 
        ref = value
        for y in range(1,9):
            for x in range(1,9):
                value = ref + self.payload.pop(0) / 10 
                fields[f'grideye_x{x}_y{y}'] = value
        return fields

    def parse_pressure(self):
        """parse pressure payload (0x14)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pressure'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 24 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['pressure'] = value / 1000
        return fields

    def parse_sound(self):
        """parse sound payload (0x15)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_sound'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) 
        fields['sound_peak'] = value
        value = self.payload.pop(0)
        fields['sound_avg'] = value
        return fields
  
    def parse_pulse_count2(self):
        """parse pulse_count2 payload (0x16)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['pulse_count2'] = value
        return fields    

    def parse_pulse_count2_abs(self):
        """parse pulse_count2_abs payload (0x17)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_pulse_count2_abs'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 24 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['pulse_count2_abs'] = value
        return fields    

    def parse_analog2(self):
        """parse analog2 payload (0x18)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_analog2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['analog2'] = value
        return fields

    def parse_external_temp2(self):
        """parse external_temp1 payload (0x19)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_temp2'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_temp2'] = value / 10
        return fields

    def parse_external_digital2(self):
        """parse external_digital2 payload (0x1A)"""
        fields = {}
        if len(self.payload) < 1:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_digital2'. "
                f"expected 1 byte, received {len(self.payload)} byte."
                f"-->skipping.")
            return fields
        value = self.payload.pop
        fields['external_digital2'] = value
        return fields

    def parse_external_analog(self):
        """parse external_analog payload (0x1B)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_external_analog'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 24 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['external_analog'] = value /1000000000
        return fields  

    def parse_debug(self):
        """parse debug payload (0x3D)"""
        fields = {}
        if len(self.payload) < 4:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_debug'. "
                f"expected 4 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = (
            self.payload.pop(0) << 24 |
            self.payload.pop(0) << 16 |
            self.payload.pop(0) << 8 |
            self.payload.pop(0))
        fields['debug'] = value
        return fields  

    def parse_settings(self):
        """parse settings payload (0x3E)"""
        fields = {}
        if len(self.payload) < 2:
            self.logger.warning(
                f"payload from device {self.devicename} too short for 'parse_settings'. "
                f"expected 2 bytes, received {len(self.payload)} bytes."
                f"-->skipping.")
            return fields
        value = self.payload.pop(0) << 8 | self.payload.pop(0)
        fields['settings'] = value
        return fields

    def _convert(self, message):
        '''
        decode payload from elsys sensors
        elsys payload example:
        0100F6 021B 030FFF3E 070E25 0C00F3 0F00 14000EA36E
        01: type temp
        00F6: temp
        02: type humidity
        1B: humidity
        ...

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
        message_json = json.loads(message.decode('utf-8'))        
        publ_array = []
        self.curr_time = int(time.time())
        self.payload = list(bytearray(base64.b64decode(message_json.get('preconverted').get('payload'))))
        entry = {}
        entry['fields'] = {}
        try:
            while len(self.payload) > 0:
                # header
                header = self.payload.pop(0)
                offset = header >> 6
                sensortype_bytes = (header & 31)
                self.logger.debug("type: {}, offset:{}".format(
                    hex(sensortype_bytes),
                    hex(offset)
                    )
                )
                sensortype = self.msg_types.get(sensortype_bytes, None)
                if sensortype:
                    method_name = "parse_"+sensortype
                    method = getattr(self, method_name, None)
                    if method:
                        sensorvalues = method()
                        self.logger.debug("method: {}, result:{}".format(
                            method,
                            sensorvalues
                        ))
                        if sensorvalues:
                            entry['fields'].update(sensorvalues)
                    else:
                        self.logger.exception(
                            "Method for {} not implemented".format(
                                method_name))
                else:
                    self.logger.error(
                        "Unknown Sensortype: {}".format(sensortype_bytes))
        except Exception:
            self.logger.exception("Error while trying to decode payload..")
        publ_array.extend([entry])
        return publ_array
