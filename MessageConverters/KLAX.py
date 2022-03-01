#!/usr/bin/python3

import time
from MessageConverters.MessageConverter import MessageConverter
import logging
from datetime import datetime

class KLAX(MessageConverter):
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
    
    # example payload
    # AGoIEQMAAAAAAAAEak2DATEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABdQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==
    # 00 6a 08 11 03 00 00 00 00 00 00 04 6a 4d 83 01 31 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 75 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
    
    def _convert(self, payload, port) :
        publ_array = []
        curr_time = int(time.time())
        try:
            params = {}
            value=(payload[0]<<8 | payload[1]) & 0x3FFF
            entry = {}
            entry["sensorid"] = "batV"
            entry["ts"] = curr_time
            entry["value"] = value/1000
            publ_array.append(entry)
            
            #SHT20,temperature,units:℃
            value=payload[2]<<8 | payload[3]
            if payload[2] & 0x80:
                value = 0xFFFF0000
            entry = {}
            entry["sensorid"] = "temp_SHT"
            entry["ts"] = curr_time
            entry["value"] = value/100
            publ_array.append(entry)

            #SHT20,Humidity,units:%
            value=payload[4]<<8 | payload[5] 
            entry = {}
            entry["sensorid"] = "hum_SHT"
            entry["ts"] = curr_time
            entry["value"] = value/10 
            publ_array.append(entry)

            #DS18B20,temperature,units:℃
            value=payload[7]<<8 | payload[8]
            if(payload[7] & 0x80):
                value = 0xFFFF0000
            entry = {}
            entry["sensorid"] = "temp_ds"
            entry["ts"] = curr_time
            entry["value"] = value/100
            publ_array.append(entry)

        except Exception as err:
            self.logger.exception("Error while trying to decode payload..")

        return publ_array
