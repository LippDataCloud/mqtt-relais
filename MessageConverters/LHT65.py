#!/usr/bin/python3

import time
from MessageConverters.MessageConverter import MessageConverter
import logging
from datetime import datetime


class LHT65(MessageConverter):
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
        payload_list = list(bytearray(payload))
        publ_array = []
        curr_time = int(time.time())
        entry = {}
        if (len(payload_list) == 0):
            self.logger.warn("message has no content - skipping.")
            return entry
        fields = {}
        try:
            # battery voltage
            value = (
                payload_list.pop(0) << 8 |
                payload_list.pop(0))
            value &= 0x3FFF
            fields['batV'] = value/1000
            
            # SHT20,temperature,units:℃
            value = (
                payload_list.pop(0) << 8 |
                payload_list.pop(0))
            # check if minus value
            if(value & 0x8000):
                value -= 1 << 16
            fields['temp_SHT'] = value/100

            # SHT20,Humidity,units:%
            value = (
                payload_list.pop(0) << 8 |
                payload_list.pop(0))
            fields['hum_SHT'] = value/10

            # ignore byte 6 (ext sensor model)
            payload_list.pop(0)

            # DS18B20,temperature,units:℃
            value = (
                payload_list.pop(0) << 8 |
                payload_list.pop(0))
             # check if minus value
            if(value & 0x8000):
                value -= 1 << 16
            fields['temp_ds'] = value/100
            
            entry['fields'] = fields
            publ_array.append(entry)
        except Exception as err:
            self.logger.exception("Error while trying to decode payload..")
        return publ_array
