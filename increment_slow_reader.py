from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")
DEBUG = c_bool.in_dll(lib, "MOMENTUM_OPT_DEBUG")

context = lib.momentum_context()
lib.momentum_configure(context, DEBUG, True)

lib.momentum_subscribed.restype = c_uint8

@CFUNCTYPE(None, POINTER(c_uint8), c_size_t, c_size_t, c_uint64)
def handle_message(data, data_length, buffer_length, msg_id):
    memory = cast(data, POINTER(c_uint8 * data_length))
    print(bytearray(memory.contents[:]).decode('utf8'))
    time.sleep(1)

STREAM = b'momentum://incrementer'

while lib.momentum_subscribe(context, STREAM, handle_message) == 0:
    time.sleep(1)

try:
    while lib.momentum_subscribed(context, STREAM, handle_message):
        time.sleep(0.1)

except KeyboardInterrupt:
    lib.momentum_term(context)
    lib.momentum_destroy(context)

