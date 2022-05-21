from ctypes import *

from momentum import Context

STREAM = b'momentum://incrementer'

context = Context()
context.debug = 1

context.subscribe(STREAM)

try:
    while (context.is_subscribed(STREAM)):
        string = context.receive_string(STREAM)
        if string is not None:
            print('Received:', string)
except KeyboardInterrupt:
    context.term()
