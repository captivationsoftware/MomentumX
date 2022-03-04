from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")

MAX_LATENCY_MS = c_int.in_dll(lib, "MOMENTUM_OPT_MAX_LATENCY")

context = lib.momentum_context()

now = time.time()
bytes_received = 0
messages_received = 0

latency_avg = 0
last_msg_id = 0
skip_count = 0

@CFUNCTYPE(None, POINTER(c_uint8), c_size_t, c_size_t, c_uint64, c_uint64)
def handle_message(data, data_length, buffer_length, msg_id, latency_ms):
    global now
    global bytes_received
    global messages_received
    global latency_avg 
    global last_msg_id
    global skip_count

    messages_received += 1
    bytes_received += data_length
    latency_avg += latency_ms

    step = msg_id - last_msg_id
    if last_msg_id > 0 and step > 1:
        skip_count = skip_count + step

    last_msg_id = msg_id

    threshold = 10

    if (messages_received % threshold == 0):
        elapsed = time.time() - now
        print("Recvd {:.2f} msgs/sec".format(messages_received / elapsed))
        print("Recvd {:.2f} MB/sec".format(bytes_received / elapsed / 1.0e6))

        print("Latency: {:.2f}ms".format(latency_avg / messages_received))
        print(f"Missed {skip_count / threshold * 100}%")
        skip_count = 0

lib.momentum_configure(context, MAX_LATENCY_MS, 10)

lib.momentum_subscribe(context, sys.argv[1].encode(), handle_message)


try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    pass

