import os

import redis
import serial


def fetch_redis_string(ieee, hash):
    command = "GETDEL lorabridge:device:{}:message:{}".format(
        ieee.decode("utf-8"), hash.decode("utf-8")
    )
    print(command)
    reply = redis_client.execute_command(command)

    if reply:
        return reply
    else:
        return None


def fetch_redis_queues():
    reply = redis_client.smembers("lorabridge:device:index")

    if reply:
        return reply
    else:
        return None


def fetch_redis_message(ieee):
    command = "ZPOPMIN lorabridge:queue:{}".format(ieee.decode("utf-8"))
    # print(command)
    reply = redis_client.execute_command(command)

    if reply:
        return reply
    else:
        return None


# Assuming redis_client is already initialized

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
)


def fetch_one_message():
    redis_queues = fetch_redis_queues()

    if redis_queues == None:
        return None

    for redis_queue in redis_queues:
        redis_msg = fetch_redis_message(redis_queue)
        if redis_msg == None:
            continue
        redis_string = fetch_redis_string(redis_queue, redis_msg[0])

        return redis_string
        break
    return None


def push_to_command_queue(lb_command: str):
    redis_client.lpush("lbcommands", lb_command)


def main():
    # Define serial port and baudrate
    serial_port = os.environ.get(
        "SERIAL_PORT", "/dev/ttyACM0"
    )  # COM4'  # Change this to your serial port
    baudrate = 115200  # Change this to match the baudrate of your device

    # Open serial connection
    ser = serial.Serial(serial_port, baudrate)

    while True:
        try:

            # Read data from serial port
            data = ser.readline().decode("utf-8").strip()
            print(data)

            if "LBDATA" in data:
                print("Data: ", data[8:], " being pushed onto command stack")
                push_to_command_queue(data[8:])

            # Transmit "tx_ok" back to the serial interface
            if "tx_token" in data:
                lb_message = fetch_one_message()
                if lb_message != None:
                    ser.write(lb_message)
                    print("Sent a message")
                else:
                    print("Queue empty, sending nothing...")
        except KeyboardInterrupt:
            print("Exiting...")
            ser.close()
            break


if __name__ == "__main__":
    main()
