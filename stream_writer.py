from ctypes import *
import sys

lib = cdll.LoadLibrary("./libmomentum.so")
MIN_BUFFERS = c_int.in_dll(lib, "MOMENTUM_OPT_MIN_BUFFERS")
DEBUG = c_bool.in_dll(lib, "MOMENTUM_OPT_DEBUG")
BLOCKING = c_bool.in_dll(lib, "MOMENTUM_OPT_BLOCKING")

context = lib.momentum_context()
lib.momentum_configure(context, DEBUG, True)

data_length = int(100e6)

STREAM = b'streamer'

i = 0
try:
    while True:
        buffer = lib.momentum_acquire_buffer(context, STREAM, data_length)

        if (buffer):
            lib.momentum_send_buffer(context, buffer, data_length, 0)


except KeyboardInterrupt:
    lib.momentum_term(context)
    lib.momentum_destroy(context)

