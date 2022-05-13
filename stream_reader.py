from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")
DEBUG = c_bool.in_dll(lib, "MOMENTUM_OPT_DEBUG")

context = lib.momentum_context()
lib.momentum_configure(context, DEBUG, True)

now = time.time()
bytes_received = 0
messages_received = 0

last_msg_id = 0
skip_count = 0

lib.momentum_subscribed.restype = c_uint8

@CFUNCTYPE(None, POINTER(c_uint8), c_size_t, c_size_t, c_uint64)
def handle_message(data, data_length, buffer_length, msg_id):
    global now
    global bytes_received
    global messages_received
    global last_msg_id
    global skip_count

    messages_received += 1
    bytes_received += data_length

    step = msg_id - last_msg_id
    if last_msg_id > 0 and step > 1:
        skip_count = skip_count + step

    last_msg_id = msg_id

    threshold = 10000

    if (messages_received % threshold == 0):
        elapsed = time.time() - now
        print("Recvd {:.2f} msgs/sec".format(messages_received / elapsed))
        print("Recvd {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))

        print(f"Missed: {skip_count}")
        skip_count = 0


STREAM = b'momentum://streamer'

while lib.momentum_subscribe(context, STREAM, handle_message) == 0:
    time.sleep(1)

try:
    while lib.momentum_subscribed(context, STREAM, handle_message):
        time.sleep(0.1)

except KeyboardInterrupt:
    lib.momentum_term(context)
    lib.momentum_destroy(context)
