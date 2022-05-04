from ctypes import *
import sys

lib = cdll.LoadLibrary("./libmomentum.so")
MIN_BUFFERS = c_int.in_dll(lib, "MOMENTUM_OPT_MIN_BUFFERS")
DEBUG = c_bool.in_dll(lib, "MOMENTUM_OPT_DEBUG")

context = lib.momentum_context()
# lib.momentum_configure(context, MIN_BUFFERS, 30)
lib.momentum_configure(context, DEBUG, True)


lib.momentum_send_data.argtypes = (c_void_p, c_char_p, c_char_p, c_size_t, c_uint64,)

data_length = int(float(sys.argv[2]))
data_bytes_1 = b'a' * data_length
data_bytes_2 = b'b' * data_length

i = 0
try:
    stream = sys.argv[1].encode('utf8')
    while True:
        data = str(i).encode()
        data_len = len(data)
        lib.momentum_send_data(context, stream, data, data_len, 0)
        i += 1

        # data[0] = str(i).encode()
        # buffer = lib.momentum_acquire_buffer(context, stream, data_length)

        # if (buffer):
        #     lib.momentum_send_buffer(context, buffer, data_length, 0)


except KeyboardInterrupt:
    lib.momentum_term(context)
    lib.momentum_destroy(context)

