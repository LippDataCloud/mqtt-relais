#!/usr/bin/python3

import time
from MessageConverters.MessageConverter import MessageConverter
import logging
from datetime import datetime

class MCF88LW12CO2(MessageConverter):
    def __init__(self):
        MessageConverter.__init__(self)

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
        publ_array = []
        try:
            params = {}


            # Time sync request
            if 0x01 == payload[0]:
                self.logger.debug("payload type: Time sync request")
                self.logger.debug("sync_id: {}".format((payload[1] << 24) | (payload[2] << 16) | (payload[3] << 8) | payload[4]))

            # Handle measurement packets
            elif 0x0e == payload[0] :
                self.logger.debug("payload type: measurement")
                # Sign-extend to 32 bits to support negative values, by shifting 24 bits
                # (16 too far) to the left, followed by a sign-propagating right shift:

                # time1
                time = self.__toTime(payload[1:5])
                # temperature_1
                value = (payload[6]<<24>>16 | payload[5]) / 100
                entry = {}
                entry["sensorid"] = "temperature"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # humidity1
                value = payload[7] / 2
                entry = {}
                entry["sensorid"] = "humidity"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # pressure1
                value = (payload[10] << 16) | (payload[9] << 8) | payload[8]
                entry = {}
                entry["sensorid"] = "pressure"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # illumination1
                value = (payload[12] << 8) | payload[11]
                entry = {}
                entry["sensorid"] = "illumination"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # voc1
                value = (payload[14] << 8) | payload[13]
                entry = {}
                entry["sensorid"] = "voc"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # co21
                value = (payload[16] << 8) | payload[15]
                entry = {}
                entry["sensorid"] = "co2"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                
                # time2
                time = self.__toTime(payload[17:21])
                # temperature2
                value = (payload[22]<<24>>16 | payload[21]) / 100
                entry = {}
                entry["sensorid"] = "temperature"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # humidity2
                value = payload[23] / 2
                entry = {}
                entry["sensorid"] = "humidity"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # pressure2
                value = (payload[26] << 16) | (payload[25] << 8) | payload[24]
                entry = {}
                entry["sensorid"] = "pressure"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # illumination2
                value = (payload[12] << 8) | payload[11]
                entry = {}
                entry["sensorid"] = "illumination"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # voc2
                value = (payload[30] << 8) | payload[29]
                entry = {}
                entry["sensorid"] = "voc"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                # co22 
                value = (payload[32] << 8) | payload[31]
                entry = {}
                entry["sensorid"] = "co2"
                entry["ts"] = time
                entry["value"] = value
                publ_array.append(entry)
                
                if payload[33] :
                    # battery_percentage
                    value = payload[33]
                    entry = {}
                    entry["sensorid"] = "battery"
                    entry["ts"] = time
                    entry["value"] = value
                    publ_array.append(entry)

        except Exception as err:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
