from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")
MIN_BUFFERS = c_int.in_dll(lib, "MOMENTUM_OPT_MIN_BUFFERS")

context = lib.momentum_context()
lib.momentum_configure(context, MIN_BUFFERS, 30)

data_bytes_1 = b'a' * int(float(sys.argv[2]))
data_bytes_2 = b'b' * int(float(sys.argv[2]))

try:
    data = data_bytes_1
    stream = sys.argv[1].encode('utf8')
    data_len = len(data)
    while True:
        if data == data_bytes_1:
            data = data_bytes_2
        else:
            data = data_bytes_1

        lib.momentum_send(context, stream, data, data_len)
        time.sleep(1)

except KeyboardInterrupt:
    pass

