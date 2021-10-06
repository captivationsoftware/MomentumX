from ctypes import *
import sys

lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

data_bytes_1 = b'\01' * int(sys.argv[2])
data_bytes_2 = b'\11' * int(sys.argv[2])

try:
    c = 0
    while True:
        data =  f'{c}'.encode('utf8')
        lib.momentum_send(context, sys.argv[1].encode('utf8'), data, len(data))
        c += 1
        # buffer = lib.momentum_acquire_buffer(context, b'foo', int(1e6))
        # local_buffer.from_buffer(buffer)
        # local_buffer[0] = c
        # lib.momentum_send(context, b'foo', buffer, len(message))

except KeyboardInterrupt:
    pass

