from ctypes import *
import time


lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context(b'reader')

@CFUNCTYPE(None, c_char_p)
def handle_message(message):
    print("received", message)

lib.momentum_subscribe(context, b'foo', handle_message)

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    pass

