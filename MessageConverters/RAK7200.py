#!/usr/bin/python3

import time
from datetime import datetime
from MessageConverters.MessageConverter import MessageConverter

class RAK7200(MessageConverter):
    msg_types = {
            0x0768: "humidity",
            0x0673: "pressure",
            0x0267: "temperature",
            0x0188: "gps",
            0x0371: "acceleration",
            0x0402: "air_resistance",
            0x0802: "battery_voltage",
            0x0586: "gyroscope",
            0x0902: "magnetometer_x",
            0x0a02: "magnetometer_y",
            0x0b02: "magnetometer_z"
    }

    def __init__(self, devicename):
        super().__init__(devicename)

    def __toTime(self, byteArray):
        year = 2000 + (byteArray[3] >> 1)
        month = ((byteArray[3] & 0x01) << 3) | (byteArray[2] >> 5)
        day = byteArray[2] & 0x1f
        hours = byteArray[1] >> 3
        minutes = ((byteArray[1] & 0x7) << 3) | (byteArray[0] >> 5)
        seconds = byteArray[0] & 0x1f
        # datetime(year, month, day, hour, minute, second, microsecond)
        date_time_obj = datetime(year, month, day, hours, minutes, seconds)
        return int(datetime.timestamp(date_time_obj))

    def _hasDownlinkMessage(self):
        return False

    def _getDownlinkMessage(self):
        return None

    def _convert(self, payload, port) :
        curr_time = int(time.time())
        payload = list(bytearray(payload))
        publ_array = []
        entry = {}
        fields = {}

        try:
            while len(payload) > 4:
                flag = (payload.pop(0)<<8 | payload.pop(0))
                self.logger.debug("found payload type: {}".format(hex(flag)))
                if flag == 0x0768: # Humidity
                    self.logger.debug("payload type: Humidity")
                    value=(payload.pop(0)) & 0x3FFF
                    fields['humidity'] = float(value/100)
                elif flag == 0x0673: # Atmospheric pressure
                    self.logger.debug("payload type: Atmospheric pressure")
                    value=(payload.pop(0)<<8 | payload.pop(0)) & 0x3FFF
                    fields['pressure'] = float(value/10)
                elif flag == 0x0267: # Temperature
                    self.logger.debug("payload type: temperature")
                    value=(payload.pop(0)<<8 | payload.pop(0)) & 0x3FFF
                    fields['temperature'] = float(value/10)
                elif flag == 0x0188: # GPS
                    self.logger.debug("payload type: gps")
                    value=(payload.pop(0)<<16 | payload.pop(0)<<8 | payload.pop(0))
                    fields['gps_lang'] = value
                    value=(payload.pop(0)<<16 | payload.pop(0)<<8 | payload.pop(0))
                    fields['gps_long'] = value
                    value=(payload.pop(0)<<16 | payload.pop(0)<<8 | payload.pop(0))
                    fields['gps_alt'] = value
                elif flag == 0x0371: # Triaxial acceleration
                    self.logger.debug("payload type: acceleration")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['acc.x'] = value
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['acc.y'] = value
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['acc.z'] = value
                elif flag == 0x0402: # air resistance
                    self.logger.debug("payload type: gasResistance")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    entry = {}
                    entry["sensorid"] = "gasResistance"
                    entry["ts"] = curr_time
                    entry["value"] = value* 0.01
                elif flag == 0x0802: # Battery Voltage
                    self.logger.debug("payload type: batV")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['batV'] = value * 0.01
                elif flag == 0x0586: # gyroscope
                    self.logger.debug("payload type: gyroscope")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['gyroscope.x'] = value * 0.01
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['gyroscope.y'] = value * 0.01
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['gyroscope.z'] = value * 0.01
                elif flag == 0x0902:  # magnetometer x
                    self.logger.debug("payload type: magnetometer x")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['magnetometer.x'] = value * 0.01
                elif flag == 0x0a02: # magnetometer y
                    self.logger.debug("payload type: magnetometer x")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['magnetometer.y'] = value * 0.01
                elif flag == 0x0b02: # magnetometer z
                    self.logger.debug("payload type: magnetometer x")
                    value=(payload.pop(0)<<8 | payload.pop(0))
                    fields['magnetometer.z'] = value* 0.01
                else:
                    self.logger.debug("payload type not implemented: {}".format(hex(flag)))
            entry['fields'] = fields
            publ_array.extend([entry])

        except Exception as err:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
