#!/usr/bin/python3

import time
import base64
import json
from MessageConverters.MessageConverter import MessageConverter


'''
example payload ttn v3:

{
  "end_device_ids": {
    "device_id": "eui-24e124128b100001",
    "application_ids": {
      "application_id": "lemt-raumsensoren"
    },
    "dev_eui": "24E124128B100001",
    "join_eui": "24E124C0002A0001",
    "dev_addr": "260B1AFB"
  },
  "correlation_ids": [
    "as:up:01FD7XDF3CCWAXC8ERWWFKXFTK",
    "ns:uplink:01FD7XDEWZ8ZQREV9NYHFBT1TR",
    "pba:conn:up:01FD03WY6A9DJVVF93JPHYFYQ3",
    "pba:uplink:01FD7XDEWQ4KMZ4PMYYDG34X50",
    "rpc:/ttn.lorawan.v3.GsNs/HandleUplink:01FD7XDEWZWG8E7E5HB35R1Y0Z",
    "rpc:/ttn.lorawan.v3.NsAs/HandleUplink:01FD7XDF3CT3VR0FJDZ0D72AMG"
  ],
  "received_at": "2021-08-16T16:41:18.188927828Z",
  "uplink_message": {
    "session_key_id": "AXtPqDjSMDh6BzrefzF9zQ==",
    "f_port": 85,
    "f_cnt": 32,
    "frm_payload": "A2f2AARofQZlPQBgAV0ABWoBAAd9OQIIfSUACXPCJQ==",
    "decoded_payload": {
      "activity": 1,
      "co2": 569,
      "humidity": 62.5,
      "illumination": 61,
      "infrared": 93,
      "infrared_and_visible": 352,
      "pressure": 966.6,
      "temperature": 24.6,
      "tvoc": 37
    },
    "rx_metadata": [
      {
        "gateway_ids": {
          "gateway_id": "packetbroker"
        },
        "packet_broker": {
          "message_id": "01FD7XDEWQ4KMZ4PMYYDG34X50",
          "forwarder_net_id": "000013",
          "forwarder_tenant_id": "ttnv2",
          "forwarder_cluster_id": "ttn-v2-ch",
          "forwarder_gateway_eui": "0015FCC23D0DD9BE",
          "forwarder_gateway_id": "eui-0015fcc23d0dd9be",
          "home_network_net_id": "000013",
          "home_network_tenant_id": "ttn",
          "home_network_cluster_id": "ttn-eu1"
        },
        "rssi": -111,
        "channel_rssi": -111,
        "snr": -2.8,
        "location": {
          "latitude": 47.18154548,
          "longitude": 9.46131912,
          "altitude": 465
        },
        "uplink_token": "eyJnIjoiWlhsS2FHSkhZMmxQYVVwQ1RWUkpORkl3VGs1VE1XTnBURU5LYkdKdFRXbFBhVXBDVFZSSk5GSXdUazVKYVhkcFlWaFphVTlwU1RWVGJXY3lWa1UxVVdKdE9ERlZWM0JTVG1wR1prbHBkMmxrUjBadVNXcHZhVkV6Vm5GWU1scG1UbGhGTW1Oc2JGcGlia1pzWVRCS1EyRXlPV2hrZVVvNUxuZERSMVJwV2pkQk1uSjFTa1V5UTAxeFgwWlRhMmN1TjFkTmRsSXlUbE5hWDFZNFFXZE9NeTVCT0ZwRFpqSTBXVmwzUjNrd05tdFRVR1paWWxsdFkwdFJjamx6TjB0WWFtRnBkMGxVUjBGNllXSXhibTVDWlVnd1kzaHBaSEZrUWpkV01FMUJjMjg1YkZKU2NFRmtXVkJGYjNCc00wSXpUbEkzZW1kb2MxRkNPRmhrT0hNeFpUSkZSVVZ3TVVacFFUQk1ZbEV0VTFWc1UwSlZVbmhLU0ZZM1dXNW1WMUpaWWt4dVMwSlJaWE5OVmxseE5WSjBSQzFaYlRsemNrMVNjM2xxVjFGU2RISlpNV1ZRWDJOSGJ6QlRkeTVaT0dsNGJEZHphRGszYm5NM1ZWcHZaSGhqWTIxQiIsImEiOnsiZm5pZCI6IjAwMDAxMyIsImZ0aWQiOiJ0dG52MiIsImZjaWQiOiJ0dG4tdjItY2gifX0="
      }
    ],
    "settings": {
      "data_rate": {
        "lora": {
          "bandwidth": 125000,
          "spreading_factor": 10
        }
      },
      "data_rate_index": 2,
      "coding_rate": "4/5",
      "frequency": "868300000"
    },
    "received_at": "2021-08-16T16:41:17.983260817Z",
    "consumed_airtime": "0.534528s",
    "version_ids": {
      "brand_id": "milesight-iot",
      "model_id": "am107",
      "hardware_version": "1.4",
      "firmware_version": "V1.25",
      "band_id": "EU_863_870"
    },
    "network_ids": {
      "net_id": "000013",
      "tenant_id": "ttn",
      "cluster_id": "ttn-eu1"
    }
  }
}

'''

class TTN_V3(MessageConverter):

    def __init__(self):
        super().__init__()

    def _convert(self, message):
        message_json = json.loads(message.decode('utf-8'))
        self.logger.info(f' converter json: {json.dumps(message_json, indent=4)}')
        ttnv3_fields = {}
        ttnv3_fields['received_at'] = message_json.get('received_at')

        ids = message_json.get('end_device_ids')
        ttnv3_fields['device_id'] = ids.get('device_id')
        ttnv3_fields['dev_eui'] = ids.get('dev_eui')

        uplink_msg = message_json.get('uplink_message')

        ttnv3_fields['payload'] = uplink_msg.get('frm_payload')
        ttnv3_fields['decoded_payload'] = uplink_msg.get('decoded_payload')

        # read only first rx metadata
        rx_metadata = uplink_msg.get('rx_metadata')[0]

        #get gateway_id if sent via packet broker
        if rx_metadata.get('packet_broker'):
          ttnv3_fields['gateway_id'] = rx_metadata.get('packet_broker').get('forwarder_gateway_id')
        else:
          ttnv3_fields['gateway_id'] = rx_metadata.get('gateway_ids').get('gateway_id')
        ttnv3_fields['rssi'] = rx_metadata.get('rssi')
        ttnv3_fields['snr'] = rx_metadata.get('snr')

        location = rx_metadata.get('location')
        if location:
          ttnv3_fields['location_lat'] = location.get('latitude')
          ttnv3_fields['location_long'] = location.get('longitude')
          ttnv3_fields['location_alt'] = location.get('altitude')
        message_json['preconverted'] = ttnv3_fields
        return json.dumps(message_json).encode('utf-8')
