#!/usr/bin/python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long,invalid-name
"""mqtt-influxdb2 - Store MQTT messages into InfluxDB v2.x.

Subscribes to a MQTT server and stores incoming messages to InfluxDB.
Configuration is done via environment variables or `.env` file.

Usage:
  mqtt_influxdb2.py [options]
  mqtt_influxdb2.py -h | --help
  mqtt_influxdb2.py --version

Arguments:
  None.

Options:
  -h --help         Show this screen.
  --logfile=FILE    Logging to FILE, otherwise use STDOUT.
  --no-color        No colored log output.
  -v --verbose      Be more verbose.
  --version         Show version.
"""
##
# LICENSE:
##
# Copyright (c) 2020-2022 by Ixtalo, ixtalo@gmail.com
##
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
##
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
##
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
##
import logging
import os
import sys
import signal
import asyncio
import re
from time import ctime, time_ns
from json import loads, dumps
from ast import literal_eval
from collections.abc import Iterable

import colorlog
from docopt import docopt
from dotenv import load_dotenv
# https://pypi.org/project/paho-mqtt/
import paho.mqtt.client as mqtt
# https://pypi.org/project/influxdb-client/
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import Point

__appname__ = "mqtt-influxdb2"
__author__ = "Ixtalo"
__email__ = "ixtalo@gmail.com"
__license__ = "AGPL-3.0+"
__version__ = "1.7.2"
__date__ = "2020-05-02"
__updated__ = "2022-10-16"
__status__ = "Production"

######################################################
######################################################
######################################################

LOGGING_STREAM = sys.stdout
DEBUG = bool(os.environ.get("DEBUG", "").lower() in ("1", "true", "yes"))

MYNAME = 'mqtt-influxdb2'
my_mqtt = None

# check for Python3
if sys.version_info < (3, 0):
    sys.stderr.write("Minimum required version is Python 3.x!\n")
    sys.exit(1)


class MyMqtt:
    """MQTT handling."""

    def __init__(self, host, port, bucket, user=None, password=None):
        """Initialize MQTT handling class."""
        self.bucket = bucket
        self.buffer_size = int(os.getenv("BUFFER_SIZE", "100"))
        self.measurement_name = os.getenv("INFLUXDB_MEASUREMENT_NAME", "message")
        self.topics = evaluate_python_string_as_iterable(os.getenv("MQTT_TOPICS_SUBSCRIBE", "#"))
        self.topics_ignore = evaluate_python_string_as_iterable(os.getenv("MQTT_TOPICS_IGNORE", "[]"))
        self.message_buffer = []
        self.__connect(host, port, user, password)

    def __connect(self, host, port, user, password):
        """Connect to MQTT server."""
        # clean_session=True: the broker will remove all information about this client when it disconnects.
        self.mqtt_client = mqtt.Client(client_id=f"{MYNAME}({__version__})", clean_session=True)
        # ##mqtt_client.subscribe(MQTT_SUBSCRIBE_TOPIC)  # done in on_connect
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_subscribe = self.on_subscribe
        self.mqtt_client.on_unsubscribe = self.on_unsubscribe
        self.mqtt_client.on_message = self.on_message
        if user is not None:
            self.mqtt_client.username_pw_set(user, password)
        if DEBUG:
            self.mqtt_client.on_log = self.on_log
        try:
            errno = self.mqtt_client.connect(host, port)
            if errno == mqtt.MQTT_ERR_SUCCESS:
                # noinspection PyProtectedMember
                # pylint: disable=protected-access
                logging.info("MQTT connected to %s:%d as '%s'", host, port,
                             self.mqtt_client._client_id)
            else:
                logging.error("MQTT problem connecting: %s", mqtt.error_string(errno))
                raise RuntimeError(f"Problem connecting to MQTT server: {mqtt.error_string(errno)}")
        except Exception as ex:
            logging.exception("Exception when connecting to MQTT server %s: %s", host, ex)
            raise ex

    def subscribe(self, client):
        """Subscribe to MQTT topics."""
        # construct list of tuples, e.g., [("topic1",0), ("topic2",0), ...]
        topics_qos = list(zip(self.topics, (0,) * len(self.topics)))
        # MQTT subscribe, multi-topic
        res, _ = client.subscribe(topics_qos)
        if res == mqtt.MQTT_ERR_SUCCESS:
            logging.debug("MQTT subscribed to '%s'", self.topics)
        else:
            logging.warning("MQTT Could not subscribe to topic '%s'. Result:%s",
                            self.topics, mqtt.error_string(res))

    def loop_forever(self, *args, **kwargs):
        """MQTT main network loop, handles reconnecting.

        This is a blocking form of the network loop and will not return until
        the client calls disconnect(). It automatically handles reconnecting.
        """
        return self.mqtt_client.loop_forever(*args, **kwargs)

    async def write_buffer(self):
        """Empty the stack and write everything to the database."""
        logging.info("Writing #%d items from buffer to DB ...", len(self.message_buffer))
        async with InfluxDBClientAsync.from_env_properties(debug=False) as client:
            write_api = client.write_api()
            await write_api.write(bucket=self.bucket, record=self.message_buffer)
        # empty queue
        self.message_buffer = []

    def on_message(self, _, __, msg):  # on_message(client, userdata, message)
        """MQTT event when a message arrives."""
        if self.topics_ignore:
            for topic_ignore in self.topics_ignore:
                if self.check_matches_pattern(topic_ignore, msg.topic):
                    logging.info("Ignoring topic '%s' because of exclusion '%s'", msg.topic, topic_ignore)
                    return

        payload = msg.payload
        if not payload:
            logging.debug("Empty message, ignoring...")
            return
        # convert binary/bytes payload to unicode string
        payload = decode_as_unicode(payload)
        if not payload:
            logging.error("Payload decoding problem! (topic '%s')", msg.topic)
            return
        # convert to InfluxDB Data Point
        datapoint = self.create_data_point(self.measurement_name, msg.topic, payload)

        # store in local buffer
        self.message_buffer.append(datapoint)
        buffer_n = len(self.message_buffer)
        logging.debug("MQTT message_buffer: #%d items", buffer_n)
        if buffer_n >= self.buffer_size:
            # buffer limit reached -> write to database
            asyncio.run(self.write_buffer())

    @staticmethod
    def create_data_point(measurement_name, topic, payload):
        """Create a InfluxDB data point."""
        # detect payload type
        payload_type = identify_type_in_string(payload)
        payload_type_name = payload_type.__name__
        logging.debug("payload_type: %s", payload_type_name)
        # parse payload
        if payload_type is int or payload_type is float:
            payload_type_name = 'float'
            payload_value = float(payload)
        elif payload_type is bool:
            payload_value = bool(payload)
        elif payload_type is dict or payload_type is list:
            try:
                # is it actually JSON? -> try to convert to JSON then back to string
                payload_value = dumps(loads(payload, parse_float=float))
                payload_type_name = 'json'
            except Exception as ex:
                # ignore exception
                logging.debug("(ignorable) payload JSON parsing problem: %s", ex)
                payload_value = str(payload)
                payload_type_name = 'str'
        else:
            payload_value = str(payload)
            payload_type_name = 'str'
        logging.debug("parsed payload (type %s): %s", payload_type_name, payload_value)
        datapoint = Point(measurement_name).tag("topic", topic).field(
            f"payload_{payload_type_name}", value=payload_value).time(time_ns())
        return datapoint

    @staticmethod
    def check_matches_pattern(pattern: str, topic: str) -> bool:
        """Check if the topic is being matched by the MQTT pattern.

        :param pattern: MQTT topic pattern (with '#' or '+')
        :param topic: topic string.
        :return: True if the pattern matches the topic.
        """
        if "#" in pattern:
            return re.match(pattern.replace("#", ".+"), topic) is not None
        elif "+" in pattern:
            return re.match(pattern.replace("+", "[^/]+"), topic) is not None
        # else: simple string comparison
        return topic == pattern

    @staticmethod
    def on_subscribe(_, __, ___, granted_qos):  # on_subscribe(client, userdata, mid, granted_qos)
        """MQTT event when subscribed to a topic."""
        logging.info("MQTT subscribed. (granted_qos:%s)", str(granted_qos))

    def on_unsubscribe(self, _, __, ___):  # on_unsubscribe(client, userdata, mid)
        """MQTT event when unsubscribed to a topic."""
        logging.warning('MQTT unsubscribed! Resubscribe...')
        self.subscribe(self.mqtt_client)

    @staticmethod
    def on_log(_, __, ___, buf):  # on_log(client, userdata, level, buf)
        """Override for MQTT logging."""
        logging.debug("MQTT %s", buf)

    def on_connect(self, client, _, __, rc):  # on_connect(client, userdata, flags, rc)
        """MQTT event for establishing a connection."""
        logging.info("MQTT connected with result code %d", rc)
        if rc == mqtt.MQTT_ERR_SUCCESS:
            # Subscribing here in on_connect() means that if we lose the
            # connection and reconnect then subscriptions will be renewed.
            self.subscribe(client)
        else:  # not mqtt.MQTT_ERR_SUCCESS
            logging.warning("MQTT connection error: %s", mqtt.error_string(rc))

    @staticmethod
    def on_disconnect(_, __, rc):  # on_disconnect(client, userdata, rc)
        """MQTT event when disconnected."""
        if rc == mqtt.MQTT_ERR_SUCCESS:
            logging.debug('MQTT disconnect successful.')
        else:
            logging.warning("MQTT disconnected! %s (%d)", mqtt.error_string(rc), rc)
            if rc == mqtt.MQTT_ERR_NOMEM:
                logging.warning('MQTT MQTT_ERR_NOMEM - is another instance running?!')


def decode_as_unicode(value: bytes) -> str:
    """Convert binary payload to a unicode string.

    :param value: binary/bytes value.
    :return: unicode/UTF8 string.
    """
    assert isinstance(value, bytes), "Input must be binary/bytes!"
    try:
        return value.decode('utf8')
    except UnicodeDecodeError as ex:
        logging.error("String decoding problem: %s", ex)
    return None


def identify_type_in_string(value: str) -> type:
    """Identify the data type inside the input string.

    :param value: string input value.
    :return: data type
    """
    if not isinstance(value, str):
        raise ValueError("Input value must be a string!")
    # https://stackoverflow.com/questions/22199741/identifying-the-data-type-of-an-input
    try:
        return type(literal_eval(value))
    except (ValueError, SyntaxError):
        # A string, so return str
        return str


def evaluate_python_string_as_iterable(value: str) -> Iterable:
    """Take a string and evaluate it as Python code of a tuple or list.

    :param value: input data.
    :return: iterable object.
    """
    if not value or (isinstance(value, str) and not value.strip()):
        raise ValueError("Value must be a valid Python list/tuple! (it's empty/null!)")
    try:
        # evaluate the string from the .env configuration as a Python tuple or list
        iterable = literal_eval(value)
    except ValueError:
        raise ValueError(f"Value must be a valid Python list/tuple! ('{value}')")
    assert isinstance(iterable, Iterable), "Value must be an iterable object!"
    return iterable


def __setup_logging(log_file: str = None, verbose=False, no_color=False):
    if log_file:
        # pylint: disable=consider-using-with
        stream = open(log_file, "a", encoding="utf8")
        no_color = True
    else:
        stream = LOGGING_STREAM
    handler = colorlog.StreamHandler(stream=stream)

    format_string = "%(log_color)s%(asctime)s %(levelname)-8s %(message)s"
    formatter = colorlog.ColoredFormatter(format_string, datefmt="%Y-%m-%d %H:%M:%S", no_color=no_color)
    handler.setFormatter(formatter)

    logging.basicConfig(level=logging.WARNING, handlers=[handler])
    if verbose or log_file:
        logging.getLogger("").setLevel(logging.INFO)
    if DEBUG:
        logging.getLogger("").setLevel(logging.DEBUG)


def _log_startup_info():
    # store the current logging level for later restore
    logger = logging.getLogger("")
    actual_loglevel = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    logger.info("log-level:%s, cwd:%s, euid:%d, egid:%d, pid:%d, MQTT buffer:%d, MQTT topics:(%s), MQTT ignore:(%s)",
                logging.getLevelName(actual_loglevel),
                os.getcwd(),
                os.geteuid(),
                os.getegid(),
                os.getpid(),
                int(os.getenv("BUFFER_SIZE")),
                ",".join(evaluate_python_string_as_iterable(os.getenv("MQTT_TOPICS_SUBSCRIBE"))),
                ",".join(evaluate_python_string_as_iterable(os.getenv("MQTT_TOPICS_IGNORE", "[]")))
                )
    logger.info("MQTT library: %s", mqtt)
    # restore log level
    logger.setLevel(actual_loglevel)


def _setup_signalling():
    """Handle system signals, e.g., SIGINT, SIGTERM, etc.

    c.f. https://en.wikipedia.org/wiki/Unix_signal
    """
    signal.signal(signal.SIGINT, _signal_handler_stop)
    signal.signal(signal.SIGTERM, _signal_handler_stop)
    signal.signal(signal.SIGHUP, _signal_handler_sync)
    signal.signal(signal.SIGUSR1, _signal_handler_sync)
    signal.signal(signal.SIGUSR2, _signal_handler_sync)
    print(f"Start time: {ctime()}")
    print(f"Press Ctrl+C to quit (PID:{os.getpid():d})")


def _signal_handler_stop(sig, frame):
    """Handle system stop signales, e.g., CTRL+C / SIGINT.

    c.f. http://stackoverflow.com/questions/1112343/how-do-i-capture-sigint-in-python
    """
    logging.debug("SIGNAL: %s, %s", sig, frame)
    message = "Ctrl+C pressed or SIGINT/SIGTERM signal received. Stopping!"
    logging.warning(message)
    print(message)
    global my_mqtt
    assert my_mqtt
    # pylint: disable=global-variable-not-assigned
    # noinspection PyUnresolvedReferences
    asyncio.run(my_mqtt.write_buffer())
    logging.info("Done. Exiting...")
    sys.exit(0)


def _signal_handler_sync(sig, frame):
    """Handle signals to do database sync."""
    logging.debug("SIGNAL: %s, %s", sig, frame)
    # pylint: disable=global-variable-not-assigned
    global my_mqtt
    assert my_mqtt
    asyncio.run(my_mqtt.write_buffer())


def main():
    """Run main program entry.

    :return: exit/return code
    """
    version_string = f"mqtt-influxdb2 {__version__} ({__updated__})"
    arguments = docopt(__doc__, version=version_string)
    arg_logfile = arguments["--logfile"]
    arg_nocolor = arguments["--no-color"]
    arg_verbose = arguments["--verbose"]

    # logging
    __setup_logging(arg_logfile, arg_verbose, arg_nocolor)
    logging.info(version_string)
    logging.debug("Command line arguments: %s", arguments)

    # load configuration environment variables from .env file
    load_dotenv()
    # check configuration
    assert os.getenv("INFLUXDB_V2_TOKEN"), "Missing/invalid configuration!"
    assert os.getenv("INFLUXDB_V2_ORG"), "Missing/invalid configuration!"
    assert os.getenv("INFLUXDB_MEASUREMENT_NAME"), "Missing/invalid configuration!"
    assert int(os.getenv("BUFFER_SIZE")), "Missing/invalid configuration!"
    assert os.getenv("MQTT_TOPICS_SUBSCRIBE"), "Missing/invalid configuration!"
    assert evaluate_python_string_as_iterable(os.getenv("MQTT_TOPICS_SUBSCRIBE")), "topics must be valid!"

    _log_startup_info()

    # MQTT connection
    global my_mqtt
    my_mqtt = MyMqtt(
        host=os.getenv("MQTT_HOST", "localhost"),
        port=int(os.getenv("MQTT_PORT", "1883")),
        bucket=os.getenv("INFLUXDB_BUCKET"),
        user=os.getenv("MQTT_USER"),
        password=os.getenv("MQTT_PASS")
    )

    # check influxdb connection
    with InfluxDBClient.from_env_properties(debug=DEBUG) as client:
        assert client.ping(), "InfluxDB not reachable!"

    _setup_signalling()

    # main loop
    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    my_mqtt.loop_forever(retry_first_connection=False)

    return 0


if __name__ == '__main__':
    if DEBUG:
        # sys.argv.append('--verbose')
        pass
    if os.environ.get("PROFILE", "").lower() in ("true", "1", "yes"):
        import cProfile
        import pstats

        profile_filename = f"{__file__}.profile"
        cProfile.run('main()', profile_filename)
        with open(f'{profile_filename}.txt', 'w', encoding="utf8") as statsfp:
            profile_stats = pstats.Stats(profile_filename, stream=statsfp)
            stats = profile_stats.strip_dirs().sort_stats('cumulative')
            stats.print_stats()
        sys.exit(0)
    sys.exit(main())
