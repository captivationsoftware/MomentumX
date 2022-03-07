from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

_exit = False

@CFUNCTYPE(None, POINTER(c_uint8), c_size_t, c_size_t, c_uint64, c_uint64)
def handle_message(data, data_length, buffer_length, msg_id, latency_ms):
    global context
    global _exit 

    with open(sys.argv[2], "wb+") as f:
        f.write(bytes(data[:data_length - 1]))

    lib.momentum_term(context)
    lib.momentum_destroy(context)
    _exit = True


lib.momentum_subscribe(context, sys.argv[1].encode(), handle_message)

while not _exit:
    time.sleep(1)


