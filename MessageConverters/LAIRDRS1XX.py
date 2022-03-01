#!/usr/bin/python3

import time
from MessageConverters.MessageConverter import MessageConverter
import logging
from datetime import datetime, timedelta

class LAIRDRS1XX(MessageConverter):

    downlinkMessage= None
    
    def __init__(self, devicename):
        super().__init__(devicename)

    def __prepareSetUTC(self):
        now_utc = datetime.utcnow()
        payload_list = []
        payload_list.append(0x02) # Message type
        payload_list.append(0x00) 
            # Mote Response Options: 
            # 0 = None 1 = Send simple configuration next uplink. 
            # 2 = Send advanced configuration next uplink.
            # 3 = Send firmware version next uplink. 
        payload_list.append(now_utc.year-2015) # Year
        payload_list.append(now_utc.month) # Month
        payload_list.append(now_utc.day) # Day
        payload_list.append(now_utc.hour) # Hours
        payload_list.append(now_utc.minute) # Minutes
        payload_list.append(now_utc.second) # Seconds
        payload_bytes = bytes(payload_list)
        self.logger.debug("SetUtcPayload: {} --- {}".format(
                payload_list,
                payload_bytes
            ))
        return payload_bytes

    def __prepareConfig(self):
        now_utc = datetime.utcnow()
        payload_list = []
        payload_list.append((3).to_bytes(1, byteorder="big")) # Message type
        payload_list.append((0).to_bytes(1, byteorder="big")) 
            # Mote Response Options: 
            # 0 = None 1 = Send simple configuration next uplink. 
            # 2 = Send advanced configuration next uplink.
            # 3 = Send firmware version next uplink. 
        payload_list.append((0).to_bytes(1, byteorder="big")) # BatteryType  --> 1: Zinc-Manganese Dioxide (Alkaline) 2: Lithium/Iron Disulfide (Primary Lithium) 
        payload_list.append((600).to_bytes(2, byteorder="big")) # ReadSensorPeriod  --> Period in seconds to read the sensor
        payload_list.append((1).to_bytes(1, byteorder="big")) # SensorAggregate  --> Number of readings to aggregate before sending on LoRa
        payload_list.append((0).to_bytes(1, byteorder="big")) # TempAlarmEnable  --> Enable temperature ALARM 
        payload_list.append((0).to_bytes(1, byteorder="big")) # HumidityAlarmEnable  --> 1: Enable humidity alarm 
        payload_list.append((0).to_bytes(1, byteorder="big")) # TempAlarmLimitLow   
        payload_list.append((0).to_bytes(1, byteorder="big")) # TempAlarmLimitHigh   
        payload_list.append((0).to_bytes(1, byteorder="big")) # RHAlarmLimitLow  
        payload_list.append((0).to_bytes(1, byteorder="big")) # RHAlarmLimitHigh
        payload_list.append((1).to_bytes(1, byteorder="big")) # LED_BLE  --> Flash period in seconds when in BLE connection, 0: No flash
        payload_list.append((65535).to_bytes(2, byteorder="big")) # LED_LoRa  --> Flash period in seconds 0:  No flash, 65535 : Tx/Rx debug mode Tx – green, Rx – oran


        payload_bytes = payload_list
        self.logger.debug("__prepareConfig payload: {} --- {}".format(
                payload_list,
                payload_bytes
            ))
        return payload_bytes
    
    def _hasDownlinkMessage(self):
        return self.downlinkMessage is not None

    def _getDownlinkMessage(self):
        return self.downlinkMessage   

    def _convert(self, payload, port) :
        self.downlinkMessage= None
        publ_array = []
        try:
            params = {}
            # Handle message 'Send Temp RH Data'
            # example payload: 01:01:26:2a:5d:18:05:00:00:00:00
            if 0x01 == payload[0] :
                self.logger.debug("payload type: Send Temp RH Data")
                # Sign-extend to 32 bits to support negative values, by shifting 24 bits
                # (16 too far) to the left, followed by a sign-propagating right shift:

                curr_time = int(time.time())
                # Options
                value = bin(payload[1])[2:]
                entry = {}
                entry["sensorid"] = "Options"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)

                # Humidity
                value = payload[2]/100 + payload[3]
                entry = {}
                entry["sensorid"] = "Humidity"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)
                
                # Temp
                value = payload[4]/100 + payload[5]
                entry = {}
                entry["sensorid"] = "Temp"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)
                
                # BatteryCapacity
                value = payload[6] * 20
                entry = {}
                entry["sensorid"] = "BatteryCapacity"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)
                
                # AlarmMsgCount
                value = payload[8]<<24>>16 | payload[7]
                entry = {}
                entry["sensorid"] = "AlarmMsgCount"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)
                
                # BacklogMsgCount
                value = payload[10]<<24>>16 | payload[9]
                entry = {}
                entry["sensorid"] = "BacklogMsgCount"
                entry["ts"] = curr_time
                entry["value"] = value
                publ_array.append(entry)
                return publ_array

            # Handle message 'Send Temp and RH Aggregated Data'
            elif 0x02 == payload[0] :
                self.__prepareConfig()
                payload_list = list(bytearray(payload))
                msgType = payload_list.pop(0)
                options = payload_list.pop(0)
                if options == 1: #request for downlink 'setUTC'
                    self.downlinkMessage = self.__prepareSetUTC()
                alarmMsgCount = payload_list.pop(0)
                backlogMsgCount = int.from_bytes( [payload_list.pop(0),payload_list.pop(0)], byteorder='big')
                batteryCapacity = payload_list.pop(0)
                numberReadings = payload_list.pop(0)
                seconds = payload_list.pop(0)<<24 | payload_list.pop(0)<<16 | payload_list.pop(0)<<8 | payload_list.pop(0)
                curr_time = datetime(2015, 1, 1) + timedelta(seconds=seconds)
                self.logger.debug(
                    "msgType: {}\n"
                    "options: {}\n"
                    "alarmMsgCount: {}\n"
                    "backlogMsgCount: {}\n"
                    "batteryCapacity: {}\n"
                    "numberReadings: {} \n"
                    "curr_time: {}".format(
                   msgType,
                   bin(options),
                   alarmMsgCount,
                   backlogMsgCount,
                   batteryCapacity,
                   numberReadings,
                   curr_time
                ) )
                for x in range(0, numberReadings):
                    publ_array.append(
                        {
                            "sensorid": "Humidity",
                            "ts": curr_time,
                            "value": payload_list.pop(0)/100 + payload_list.pop(0)
                        }
                    )
                    publ_array.append(
                        {
                            "sensorid": "Temp",
                            "ts": curr_time,
                            "value": payload_list.pop(0)/100 + payload_list.pop(0)
                        }
                    )
                return []
            elif 0x05 == payload[0] : # SendSensorConfigSimple
                payload_list = list(bytearray(payload))
                msgType = payload_list.pop(0)
                options = payload_list.pop(0)
                if options == 1: #request for downlink 'setUTC'
                    self.downlinkMessage = self.__prepareSetUTC()
                batteryType = payload_list.pop(0)
                readSensorPeriod = int.from_bytes( [payload_list.pop(0),payload_list.pop(0)], byteorder='big')
                sensorAggregate = payload_list.pop(0)
                tempAlarmEnabled = payload_list.pop(0)
                humidityAlarmEnabled = payload_list.pop(0)
                self.logger.debug(
                    "msgType: {}\n"
                    "options: {}\n"
                    "batteryType: {}\n"
                    "readSensorPeriod: {}\n"
                    "sensorAggregate: {}\n"
                    "tempAlarmEnabled: {}\n"
                    "humidityAlarmEnabled: {}".format(
                   msgType,
                   bin(options),
                   batteryType,
                   readSensorPeriod,
                   sensorAggregate,
                   tempAlarmEnabled,
                   humidityAlarmEnabled
                   ))
                return []
            else:
                self.logger.warn("unknown payload type: {}".format(payload[0]))

        except Exception as err:
            self.logger.exception("Error while trying to decode payload_list..")
        self.logger.debug("********** LAIRD***** : {}".format(publ_array))
         