import os

import redis
import serial
import time
import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

lbdata_types = {
    "data": b"\x07",
    "timesync_req": b"\x01",
    "lorabridge:events:system": b"\x02",
    "lorabridge:events:user": b"\x03",
    "lorabridge:flows:digests": b"\x04",
    "lorabridge:device:join": b"\x05",
    "heartbeat": b"\x06",
    "lorabridge:device:name": b"\x08",
}

SIMPLE_QUEUES = [
    # "lorabridge:launchpad",
    "lorabridge:flows:digests",
    "lorabridge:device:name",
    "lorabridge:device:join",
    "lorabridge:events:user",
    "lorabridge:events:system",
]

NODERED_HOST = "node-red:1880"
NODERED_INTERVAL = 30.0

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
)


# def check_nodered_state():
#     threading.Timer(NODERED_INTERVAL, check_nodered_state).start()
#     conn = http.client.HTTPConnection(NODERED_HOST)
#     conn.request("GET", "/flows/state")
#     response = conn.getresponse()
#     conn.close()
#     if response.status == 200:
#         print("Retrieving nodered runtime successful")
#         state = json.loads(response.read())
#         print(state)
#         if state["state"] == "stop":
#             start_nodered_runtime()
#     else:
#         print("Error: Retrieving nodered runtime failed!")


class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(f'{{"state":"{"stop" if timesync_ongoing else "start"}"}}'.encode())
        else:
            self.send_response(404)
            self.end_headers()


def run_server():
    server_address = ("", 8000)  # Listen on port 8000
    httpd = HTTPServer(server_address, SimpleHandler)
    print("Serving on port 8000...")
    httpd.serve_forever()


def populate_uplink_launchpad():
    if redis_devices := redis_client.smembers("lorabridge:device:index"):
        for redis_device in redis_devices:
            # redis_msg = fetch_redis_message(redis_device)
            if redis_msg := redis_client.zpopmin(
                "lorabridge:queue:{}".format(redis_device.decode("utf-8"))
            ):

                lb_measurement = redis_client.getdel(
                    "lorabridge:device:{}:message:{}".format(
                        redis_device.decode("utf-8"), redis_msg[0][0].decode("utf-8")
                    )
                )
                if lb_measurement:
                    redis_client.lpush("lorabridge:launchpad", lb_measurement)


def fetch_one_message() -> str | None:

    # Priority order: Critical system events, digests, join events, sensor data

    for queue in SIMPLE_QUEUES:
        if digest_value := redis_client.lpop(queue):
            return {"type": lbdata_types[queue], "payload": digest_value}

    if launchpad_entry := redis_client.lpop("lorabridge:launchpad"):
        return {"type": lbdata_types["data"], "payload": launchpad_entry}
    else:  # If launchpad is empty, populate queue and try to fetch again
        populate_uplink_launchpad()
        if launchpad_reentry := redis_client.lpop("lorabridge:launchpad"):
            return {"type": lbdata_types["data"], "payload": launchpad_reentry}

    return None


def fetch_and_compress_lbdata() -> str | None:
    # lb_data = fetch_one_message()
    if lb_data := fetch_one_message():
        return lb_data["type"] + lb_data["payload"]


def push_to_command_queue(lb_command: str) -> None:
    redis_client.lpush("lbcommands", lb_command)


def start_nodered_runtime():
    conn = http.client.HTTPConnection(NODERED_HOST)
    conn.request(
        "POST", "/flows/state", json.dumps({"state": "start"}), {"Content-Type": "application/json"}
    )
    response = conn.getresponse()
    conn.close()
    if response.status == 200:
        print("Starting nodered runtime successful")
    else:
        print("Error: Starting nodered runtime failed!")


timesync_ongoing = True


def main():
    global timesync_ongoing
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    # Define serial port and baudrate
    serial_port = os.environ.get(
        "SERIAL_PORT", "/dev/ttyACM0"
    )  # COM4'  # Change this to your serial port
    baudrate = 115200  # Change this to match the baudrate of your device

    # Open serial connection
    ser = serial.Serial(serial_port, baudrate)

    heartbeat_time_start = time.time()
    heartbeat_interval = 60

    timesync_requested = False

    while True:
        # Read data from serial port
        data = ser.readline().decode("utf-8").strip()
        print(data)

        if "LBDATA" in data:
            print("Data: ", data[8:], " being pushed onto command stack")
            push_to_command_queue(data[8:])

        if "LBTIME" in data:
            print(
                "Updating system time with an epoch value got from LoRaWAN timesync response:",
                data[8:],
            )
            date_call_cmd = "date -s '@" + data[8:] + "'"
            os.system(date_call_cmd)
            timesync_ongoing = False
            start_nodered_runtime()
            # check_nodered_state()

        if "tx_token" in data and timesync_ongoing and timesync_requested == False:
            ser.write(lbdata_types["timesync_req"])
            timesync_requested = True

        # Transmit "tx_ok" back to the serial interface

        elif "tx_token" in data and timesync_ongoing == False:
            lb_message = fetch_and_compress_lbdata()
            if lb_message != None:
                ser.reset_output_buffer()
                ser.write(lb_message)
                print("Sent a message")
                heartbeat_time_start = time.time()
            else:
                if time.time() - heartbeat_time_start > heartbeat_interval:
                    ser.reset_output_buffer()
                    ser.write(lbdata_types["heartbeat"])
                    print("Sent a heartbeat message")
                    heartbeat_time_start = time.time()
                else:
                    print("Queue empty, sending nothing...")


if __name__ == "__main__":
    main()
