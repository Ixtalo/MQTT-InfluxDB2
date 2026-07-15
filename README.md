# !! DEPRECATED !!

There are other established tools to address this, e.g., Telegraf:

- https://docs.influxdata.com/telegraf/v1/input-plugins/mqtt_consumer/
- https://github.com/influxdata/telegraf/blob/master/plugins/processors/starlark/


# ~~mqtt-influxdb~~ !! DEPRECATED !!

Store MQTT messages into InfluxDB v2, a MQTT-InfluxDB-Bridge.


## Requirements

* Python 3.7+
* pipenv
* MQTT server, e.g., Mosquitto
* InfluxDB server, Version 2.x


## Getting Started

1. Set up MQTT and InfluxDB v2 servers.
    * For testing purposes you can use the Docker-Compose script: `cd ./scripts/docker-mqtt-influxdb/ && docker-compose up -d`.
2. `pipenv sync`
3. Create local dotenv configuraiton or setup environment variables.
    1. `cp env.template .env && chmod go-rwx .env`
    2. Adjust configuration in `.env` file.
4. `pipenv run python mqtt_influxdb2.py`
5. (Install as systemd-service, see `./scripts/systemd/`)
