from ctypes import *

from momentum import Context

def on_read(string, ts, iteration):
    print(string)

STREAM = b'momentum://incrementer'

context = Context()
context.debug = 1

context.subscribe(STREAM)

try:
    while (context.is_subscribed(STREAM)):
        context.receive_string(STREAM, on_read)
except KeyboardInterrupt:
    context.term()
