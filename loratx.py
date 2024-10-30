import os

import redis
import serial
import time
import json

lbdata_types = {
    "data":0,
    "timesync_req": 1,
    "system_event": 2,
    "user_event": 3,
    "lbflow_digest": 4,
    "lbdevice_join": 5,
    "hearthbeat": 6
}

def fetch_redis_string(ieee: str, hash: str) -> str | None:
    command = "GETDEL lorabridge:device:{}:message:{}".format(
        ieee.decode("utf-8"), hash.decode("utf-8")
    )
    print(command)
    if reply := redis_client.execute_command(command):
        return reply


def fetch_redis_queues() -> str | None:
    if reply := redis_client.smembers("lorabridge:device:index"):
        return reply


def fetch_redis_message(ieee: str) -> str | None:
    command = "ZPOPMIN lorabridge:queue:{}".format(ieee.decode("utf-8"))
    if reply := redis_client.execute_command(command):
        return reply


redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
)


def fetch_one_message() -> str | None:
    redis_queues = fetch_redis_queues()

    if redis_queues != None:
        for redis_queue in redis_queues:
            redis_msg = fetch_redis_message(redis_queue)
            if redis_msg == None:
                continue
            return fetch_redis_string(redis_queue, redis_msg[0])

def fetch_lbdata() -> dict | None:
    lb_data_string = fetch_one_message()
    lb_data = {}

    if lb_data_string != None:
        try:
            lb_data = json.loads(lb_data_string)
        except ValueError as e:
            print("Error: LB Data string has invalid format")
            return None
        return lb_data

def fetch_and_compress_lbdata() -> str | None:
    
    lb_data = fetch_lbdata()

    lb_data_key = list(lb_data.keys())[0]

    if lb_data_key not in lbdata_types.keys():
        print("Error: LB data type not found")
        return None
    
    lb_compressed_data = (str(lbdata_types[lb_data_key])+str(lb_data[lb_data_key])).encode()

    return lb_compressed_data


def push_to_command_queue(lb_command: str) -> None:
    redis_client.lpush("lbcommands", lb_command)


def main():
    # Define serial port and baudrate
    serial_port = os.environ.get(
        "SERIAL_PORT", "/dev/ttyACM0"
    )  # COM4'  # Change this to your serial port
    baudrate = 115200  # Change this to match the baudrate of your device

    # Open serial connection
    ser = serial.Serial(serial_port, baudrate)

    heartbeat_time_start = time.time
    heartbeat_interval = 60

    timesync_ongoing = False
    timesync_requested = False

    while True:
        # Read data from serial port
        data = ser.readline().decode("utf-8").strip()
        print(data)

        if "LBDATA" in data:
            print("Data: ", data[8:], " being pushed onto command stack")
            push_to_command_queue(data[8:])

        if "LBTIME" in data:
            print("Updating system time with an epoch value got from LoRaWAN timesync response:", data[8:])
            date_call_cmd = "date -d \'@"+data[8:]+"\'"
            os.system(date_call_cmd)
            timesync_ongoing = False


        if "tx_token" in data and timesync_ongoing and timesync_requested == False:
            ser.write(str(lbdata_types["timesync_req"]).encode());
            timesync_requested = True
        # Transmit "tx_ok" back to the serial interface
        elif "tx_token" in data and timesync_ongoing == False:
            lb_message = fetch_and_compress_lbdata()
            if lb_message != None:
                ser.write(lb_message)
                print("Sent a message")
            else:
                if time.time() - heartbeat_time_start > heartbeat_interval:
                    heartbeat_time_start = time.time()
                    ser.write("hb")
                    print("Sent a heartbeat message")
                else:
                    print("Queue empty, sending nothing...")


if __name__ == "__main__":
    main()
