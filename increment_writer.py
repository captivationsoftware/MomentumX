from ctypes import *
import sys

lib = cdll.LoadLibrary("./libmomentum.so")
MIN_BUFFERS = c_int.in_dll(lib, "MOMENTUM_OPT_MIN_BUFFERS")
DEBUG = c_bool.in_dll(lib, "MOMENTUM_OPT_DEBUG")
BLOCKING = c_bool.in_dll(lib, "MOMENTUM_OPT_BLOCKING")

context = lib.momentum_context()
lib.momentum_configure(context, DEBUG, True)
lib.momentum_configure(context, BLOCKING, True)

lib.momentum_send_data.argtypes = (c_void_p, c_char_p, c_char_p, c_size_t, c_uint64,)

STREAM = b'incrementer'

try:
    i = 0
    while True:
        data = str(i).encode()
        data_len = len(data)
        lib.momentum_send_data(context, STREAM, data, data_len, 0)
        i += 1


except KeyboardInterrupt:
    lib.momentum_term(context)
    lib.momentum_destroy(context)

