from ctypes import *
import sys

lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

data_bytes_1 = b'\01' * int(sys.argv[2])
data_bytes_2 = b'\11' * int(sys.argv[2])

try:
    data = data_bytes_1
    while True:
        if data == data_bytes_1:
            data = data_bytes_2
        else:
            data = data_bytes_1
        lib.momentum_send(context, sys.argv[1].encode('utf8'), data, len(data))

except KeyboardInterrupt:
    pass

