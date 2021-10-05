from ctypes import *
import time


lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

try:
    c = 0
    while True:
        c += 1
        data_bytes = f'{c}_'.encode() + b'\01' * int(1e7)
        lib.momentum_send(context, b'foo', data_bytes, len(data_bytes))
        

except KeyboardInterrupt:
    pass

