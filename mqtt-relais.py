# !/usr/bin/python3

import string
import time
import random
import json
import yaml
import ssl
import base64
import logging
from logging.config import fileConfig
import importlib
import argparse
import os
import re
from rich.logging import RichHandler


from datetime import datetime
import paho.mqtt.client as mqtt

from MessageConverters.MessageConverter import MessageConverter

LOGGING_CONFIG = 'logging.conf'
CONVERTERS_DIR = 'MessageConverters'

# list to store all mqtt connection infos
brokers = []


def _load_converter(converter_classname: string):
    if converter_classname in converters:
        logger.info(f'converter {converter_classname} is already loaded. skipping..')
    else:
        try:
            m_name = f'{CONVERTERS_DIR}.{converter_classname}'
            module = importlib.import_module(m_name)
            device_class = getattr(module, converter_classname)
            message_converter = device_class()
        except ImportError as err:
            logger.error(f'failed to load module: {converter_classname}. message: {err}')  
            return None
        logger.info(f'successfully loaded converter {device_class.__name__}')
        converters[converter_classname] = message_converter
    


def _convert_message(message: bytes, converter_classname: string) -> bytes:
    # get corresponding decoder
    message_converter = converters.get(converter_classname)
    if message_converter:
        return message_converter.convert(message)
    else:
        logger.error(f"can't find converter with name {converter_classname}. skipping..")
        return message



'''
def translate_to_tb_format(payload):
    tb_payload = {}
    measurements = []
    measurement = {}
    measurement['ts'] = payload.get('ts')
    measurement['values'] = payload.get('fields')
    deviceid = payload.get('tags').get('deviceid')
    measurements.append(measurement)    
    tb_payload[deviceid] = measurements
    return tb_payload
'''


def on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True
        client.disconnect_flag = False
        logger.info(
            f'Connect for Client {userdata.get("name")} successful.')
        for route in userdata.get('routes'):
            topic = route.get(
                "subscribe-topic")
            if topic:
                logger.info(
                    f'Subscribing to topic {topic}')
                client.subscribe(topic)
    else:
        logger.error(
            f"Connect for Client {userdata.get('name')} failed with result code: {str(rc)}")


def on_disconnect(client, userdata, rc):
    client.connected_flag = False
    client.disconnect_flag = True
    logger.info(
        "Disconnected client {} . Reason: {}".format(client, str(rc)))


def on_publish(client, userdata, result):
    # Todo after published data, remove from DB.
    logger.info(
        "data published to client {}. userdata: {} result: {}".format(
            client._client_id, userdata, result))


def on_message(client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
    logger.info(
        f"**** new message received from broker '{userdata.get('name')}' on topic '{message.topic}'")
    message_payload = message.payload
    logger.debug(
        f"received message: {message_payload.decode('utf-8')}")
    # find matching routing
    routes_to_process = []
    for route in userdata.get('routes'):
        pattern = re.compile("^" + route.get('subscribe-topic').replace('+','.*') +"$")
        if pattern.match(message.topic):
            routes_to_process.append(route)
            
    if routes_to_process:
        for route in routes_to_process:
            # convert with subscribe-converter if conigured
            subscribe_converter = userdata.get('subscribe-converter')
            if subscribe_converter:
                logger.debug(
                    f'converting message with subscribe-converter {subscribe_converter}')
                message_payload = _convert_message(
                    message_payload, subscribe_converter)
            # convert with payload-converter if conigured
            payload_converter = route.get('payload-converter')
            if payload_converter:
                logger.debug(
                    f'converting message with payload-converter {payload_converter}')
                message_payload = _convert_message(
                    message_payload, payload_converter)
            # convert with publish-converter if configured
            publish_broker = route.get('publish-broker')
            publish_client = active_clients.get(publish_broker)
            publish_converter = configuration.get("brokers").get(
                publish_broker).get('publish-converter')
            if publish_converter:
                logger.debug(
                    f'converting message with publish_converter {publish_converter}')
                message_payload = _convert_message(
                    message_payload, publish_converter)
            # publish message
            try:
                logger.info(
                    f"publishing message to broker '{route.get('publish-broker')}' on topic '{route.get('publish-topic')}'")
                logger.debug(
                    f"message: {message_payload.decode('utf-8')}")
                publish_client.publish(
                    route.get('publish-topic'),
                    payload=message_payload)
            except Exception as error:
                logger.exception(error)
    else:
        logger.info(
            f'no route found for topic {message.topic}')


def on_log(client, userdata, level, buf):
    logger.info('Loglevel: {}. message: {}, userdata: {}'.format(
        level, buf, userdata))


def connect_mqtt(name, broker_info):
    try:
        ssl.match_hostname = lambda cert, hostname: True
        auth_conf = broker_info.get("auth")
        auth_type = auth_conf.get("auth_type")
        # generate client id
        client_id = "{}-{}".format(
            name,
            random.randint(150, 205))

        # create client object
        client = mqtt.Client(
            client_id,
            clean_session=False)
        client.connected_flag = False
        client.disconnect_flag = True

        # configure authentication
        if (auth_type == "password"):
            client.username_pw_set(
                auth_conf.get("user"),
                auth_conf.get("pw"))
        elif (auth_type == "cert"):
            client.tls_set(
                ca_certs=auth_conf.get("ca_certs_path"),
                certfile=auth_conf.get("certfile_path"),
                keyfile=auth_conf.get("keyfile_path"),
                cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_SSLv23)
            client.tls_insecure_set(True)
        logger.info(
            f'connecting to broker {name} on host {broker_info.get("host")}')
        client.connect(
            host=broker_info.get("host"),
            port=broker_info.get("port"),
            keepalive=60)

        # workaround to make sure the session is clean on startup but remains on automatic reconnect
        client.disconnect()
        while client.connected_flag:
            logger.warning("waiting for client to disconnect..")
            time.sleep(1)
        client.clean_session = True
        client.connect(
            host=broker_info.get("host"),
            port=broker_info.get("port"),
            keepalive=60)
        return client
    except Exception:
        logger.error(f"connection for broker {name} failed. skipping this one..")
        return None
    


def disconnect_mqtt(client):
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity",
        action="store_true")
    parser.add_argument(
        "--conf_file",
        help="configuration file",
        type=str,
        default="config.yaml")

    args = parser.parse_args()
    path_log_config_file = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'conf', LOGGING_CONFIG)
    print(f'logging config file: {path_log_config_file}')
    fileConfig(path_log_config_file)
    logger = logging.getLogger(__name__)
    logger.info("using logging conf from {}".format(path_log_config_file))
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("verbosity turned on")

    # load config
    path_config_file = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'conf', args.conf_file)
    with open(path_config_file) as yaml_conf_file:
        configuration = yaml.full_load(yaml_conf_file)

    logger.info("loaded config: {}".format(configuration))

    
    # start all mqtt connections
    logger.info('starting mqtt connections...')
    # list to stor all active vlients
    active_clients = {}
    # dictionary to store all dynamically loaded converters
    converters = {}
    for name, conf in configuration.get("brokers").items():
        logger.info(
            f'starting client for broker {name}, connecting to host {conf.get("host")}')
        client = connect_mqtt(name, conf)
        if client:
            # Bind function to callback
            client.on_publish = on_publish
            client.on_log = on_log
            client.on_message = on_message
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.loop_start()
            client.enable_logger(logger)
            # create converter and routing info
            converter_and_routing_info = {}
            converter_and_routing_info['name'] = name
            subscribe_converter = conf.get('subscribe-converter')
            converter_and_routing_info['subscribe-converter'] = subscribe_converter
            if subscribe_converter:
                _load_converter(subscribe_converter)
            publish_converter = conf.get('publish-converter')
            converter_and_routing_info['publish-converter'] = publish_converter
            if publish_converter:
                _load_converter(publish_converter)
            converter_and_routing_info['routes'] = []
            for route in configuration.get("routing"):
                if route["subscribe-broker"] == name:
                    converter_and_routing_info['routes'].append(route)
                    payload_converter = route.get('payload-converter')
                    if payload_converter:
                        _load_converter(
                            payload_converter)
                    logger.debug(f"added route {route['name']}")
            client.user_data_set(converter_and_routing_info)
            active_clients[name] = client
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('interrupted!')
        for name, client in active_clients.items():
            disconnect_mqtt(client)
