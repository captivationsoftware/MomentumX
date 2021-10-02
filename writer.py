from ctypes import *
import time


lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context(b'writer')

big_data = b'\x01' * 4097
try:
    c = 0
    while True:
        c += 1
        data = f'This is some data: {c}'
        if c == 5000000:
            data_bytes = big_data
        else:
            data_bytes = data.encode()
        lib.momentum_send(context, b'foo', data_bytes, len(data_bytes))

except KeyboardInterrupt:
    pass

