from ctypes import *
import time


momentum = cdll.LoadLibrary("./libmomentum.so")

context = momentum.Momentum_context(b'writer')

try:
    c = 0
    while True:
        c += 1
        data = f'This is some data: {c}'
        data_bytes = data.encode()
        momentum.Momentum_send(context, b'foo', data_bytes, len(data_bytes))

except KeyboardInterrupt:
    pass

