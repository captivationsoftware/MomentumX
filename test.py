from ctypes import *
import time


momentum = cdll.LoadLibrary("./libmomentum.so")

context = momentum.context()


@CFUNCTYPE(None, c_char_p)
def handle_message(message):
    print("received", message)

momentum.subscribe(context, b'foo', handle_message)
momentum.subscribe(context, b'bar', handle_message)

try:
    while True:
        momentum.send(context, b'foo', b'this is some data on foo')

        momentum.send(context, b'bar', b'this is some data on bar')
        time.sleep(1)

except KeyboardInterrupt:
    pass

