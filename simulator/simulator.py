import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
PUBLISH_INTERVAL = max(1, int(os.getenv("PUBLISH_INTERVAL", "5")))
ANOMALY_EVERY_SECONDS = max(0, int(os.getenv("ANOMALY_EVERY_SECONDS", "180")))

rooms = {
    "room1": {"temperature": 22.5, "humidity": 45.0},
    "room2": {"temperature": 24.1, "humidity": 52.0},
    "room3": {"temperature": 21.8, "humidity": 48.0},
}
running = True
last_anomaly = time.monotonic()
anomaly_room = "room1"
anomaly_active = False
anomaly_until = 0.0

def stop_handler(_signum, _frame):
    global running
    running = False

signal.signal(signal.SIGTERM, stop_handler)
signal.signal(signal.SIGINT, stop_handler)

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="pr7-climate-simulator",
    protocol=mqtt.MQTTv311,
)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("Connected to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)
    else:
        logging.error("MQTT connection failed: %s", reason_code)

client.on_connect = on_connect

def publish_value(topic: str, value: float):
    payload = f"{value:.2f}"
    info = client.publish(topic, payload=payload, qos=0, retain=True)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        logging.warning("Publish failed for %s: rc=%s", topic, info.rc)

def main():
    global last_anomaly, anomaly_room, anomaly_active, anomaly_until
    while running:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as exc:
            logging.warning("Broker not ready (%s); retrying in 3 seconds", exc)
            time.sleep(3)

    client.loop_start()
    try:
        while running:
            now = time.monotonic()

            for name, values in rooms.items():
                values["temperature"] = min(
                    28.0, max(18.0, values["temperature"] + random.uniform(-0.18, 0.18))
                )
                values["humidity"] = min(
                    65.0, max(30.0, values["humidity"] + random.uniform(-0.45, 0.45))
                )

            if ANOMALY_EVERY_SECONDS and not anomaly_active and now - last_anomaly >= ANOMALY_EVERY_SECONDS:
                anomaly_room = random.choice(list(rooms.keys()))
                rooms[anomaly_room]["temperature"] += 6.5
                anomaly_active = True
                anomaly_until = now + PUBLISH_INTERVAL
                last_anomaly = now
                logging.warning("DEMO ANOMALY injected into %s", anomaly_room)

            for room, values in rooms.items():
                publish_value(f"pr7/{room}/temperature", values["temperature"])
                publish_value(f"pr7/{room}/humidity", values["humidity"])

            status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "rooms": rooms,
            }
            client.publish(
                "pr7/status",
                json.dumps(status),
                qos=0,
                retain=True,
            )
            logging.info(
                "Published: " + ", ".join(
                    f"{r}={v['temperature']:.1f}C/{v['humidity']:.1f}%"
                    for r, v in rooms.items()
                )
            )

            if anomaly_active and now >= anomaly_until:
                rooms[anomaly_room]["temperature"] -= 6.5
                anomaly_active = False

            time.sleep(PUBLISH_INTERVAL)
    finally:
        client.loop_stop()
        client.disconnect()
        logging.info("Simulator stopped")

if __name__ == "__main__":
    main()
