import os

import redis
import serial
import time
import json
import base64

lbdata_types = {
    "data": b"\x07",
    "timesync_req": b"\x01",
    "system_event": b"\x02",
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
]

# TODO: Replace these redis functions with one


# def fetch_simple_queue(key:str) -> str | None:
#     # command = "LPOP lorabridge:flows:digests"
#     return redis_client.lpop(key)
# if reply := redis_client.lpop(key):
#     return reply
# else:
#     return None


# def fetch_redis_flow_digest() -> str | None:
#     command = "LPOP lorabridge:flows:digests"

#     if reply := redis_client.execute_command(command):
#         return reply
#     else:
#         return None

# def fetch_redis_device_join() -> str | None:
#     command = "LPOP lorabridge:device:join"

#     if reply := redis_client.execute_command(command):
#         return reply
#     else:
#         return None

# def fetch_redis_user_event() -> str | None:
#     command = "LPOP lorabridge:events:user"

#     if reply := redis_client.execute_command(command):
#         return reply
#     else:
#         return None


# def fetch_redis_string(ieee: str, hash: str) -> str | None:
#     command = "GETDEL lorabridge:device:{}:message:{}".format(
#         ieee.decode("utf-8"), hash.decode("utf-8")
#     )
#     print(command)
#     if reply := redis_client.execute_command(command):
#         return reply


# def fetch_redis_queues() -> str | None:
#     if reply := redis_client.smembers("lorabridge:device:index"):
#         return reply


# def fetch_redis_message(ieee: str) -> str | None:
#     command = "ZPOPMIN lorabridge:queue:{}".format(ieee.decode("utf-8"))
#     if reply := redis_client.execute_command(command):
#         return reply


redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
)


def fetch_one_message() -> str | None:

    # Priority order: Critical system events, digests, join events, sensor data

    for queue in SIMPLE_QUEUES:
        if digest_value := redis_client.lpop(queue):
            return {"type": lbdata_types[queue], "payload": digest_value}

    # digest_value = fetch_redis_flow_digest()
    # if digest_value != None:
    #     return digest_value

    # join_value = fetch_redis_device_join()
    # if join_value != None:
    #     return bytes(join_value)

    # user_event_value = fetch_redis_user_event()
    # if user_event_value != None:
    #     return user_event_value

    if redis_devices := redis_client.smembers("lorabridge:device:index"):
        for redis_device in redis_devices:
            # redis_msg = fetch_redis_message(redis_device)
            if redis_msg := redis_client.zpopmin(
                "lorabridge:queue:{}".format(redis_device.decode("utf-8"))
            ):
                return {
                    "type": lbdata_types["data"],
                    # "payload": fetch_redis_string(redis_device, redis_msg[0]),
                    "payload": redis_client.getdel(
                        "lorabridge:device:{}:message:{}".format(
                            redis_device.decode("utf-8"), redis_msg[0][0].decode("utf-8")
                        )
                    ),
                }
            # if redis_msg == None:
            #     continue
            # return {
            #     "type": lbdata_types["data"],
            #     "payload": fetch_redis_string(redis_device, redis_msg[0]),
            # }


# def fetch_lbdata() -> dict | None:
#     lb_data_string = fetch_one_message()
#     lb_data = {}

#     print("lbdata fetch got: ", lb_data_string)

#     if lb_data_string != None:
#         try:
#             lb_data = json.loads(lb_data_string)
#         except ValueError as e:
#             print("Error: LB Data string has invalid format")
#             return None
#         return lb_data


def fetch_and_compress_lbdata() -> str | None:
    # lb_data = fetch_one_message()
    if lb_data := fetch_one_message():
        return lb_data["type"] + lb_data["payload"]
    # lb_data = fetch_lbdata()

    # if lb_data == None:
    #     return

    # lb_data_key = list(lb_data.keys())[0]

    # if lb_data_key not in lbdata_types.keys():
    #     print("Error: LB data type not found")
    #     return

    # if lb_data_key == "data":
    #     lb_compressed_data = lbdata_types[lb_data_key].to_bytes(
    #         length=1, byteorder="big"
    #     ) + base64.b64decode(lb_data[lb_data_key])
    # elif lb_data_key == "lbdevice_join":
    #     lb_compressed_data = (
    #         lbdata_types[lb_data_key].to_bytes(length=1, byteorder="big")
    #         + lb_data[lb_data_key].encode()
    #     )
    # else:
    #     lb_compressed_data = (
    #         lbdata_types[lb_data_key].to_bytes(length=1, byteorder="big")
    #         + lb_data[lb_data_key]
    #     )

    # return lb_compressed_data


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

    heartbeat_time_start = time.time()
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
            print(
                "Updating system time with an epoch value got from LoRaWAN timesync response:",
                data[8:],
            )
            date_call_cmd = "date -d '@" + data[8:] + "'"
            os.system(date_call_cmd)
            timesync_ongoing = False

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
                    ser.write(lbdata_types["heartbeat"])
                    print("Sent a heartbeat message")
                    heartbeat_time_start = time.time()
                else:
                    print("Queue empty, sending nothing...")


if __name__ == "__main__":
    main()
