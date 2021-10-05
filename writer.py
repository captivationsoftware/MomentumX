from ctypes import *
import time


lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

data_bytes_1 = b'\01' * int(4e4)
data_bytes_2 = b'\11' * int(4e4)

try:
    c = 0
    while True:
        data =  f'{c}'.encode('utf8')
        lib.momentum_send(context, b'foo', data, len(data))
        c += 1
        # buffer = lib.momentum_acquire_buffer(context, b'foo', int(1e6))
        # local_buffer.from_buffer(buffer)
        # local_buffer[0] = c
        # lib.momentum_send(context, b'foo', buffer, len(message))

except KeyboardInterrupt:
    pass

