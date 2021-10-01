from ctypes import *
import time


momentum = cdll.LoadLibrary("./libmomentum.so")

context = momentum.context(b'reader')

@CFUNCTYPE(None, c_char_p)
def handle_message(message):
    print("received", message)

momentum.subscribe(context, b'foo', handle_message)

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    pass

