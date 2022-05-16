from ctypes import *

from momentum import Context

def on_read(data, data_length, ts, iteration):
    print(data)

STREAM = b'momentum://incrementer'

context = Context()
context.debug = 1

context.subscribe(STREAM)

try:
    while (context.is_subscribed(STREAM)):
        context.read(STREAM, on_read)
except KeyboardInterrupt:
    context.term()
