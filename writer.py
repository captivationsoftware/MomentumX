from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

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

except KeyboardInterrupt:
    pass

