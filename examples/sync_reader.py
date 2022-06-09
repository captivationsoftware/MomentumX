from ctypes import *
import time

from momentum import Context, LogLevel

STREAM = b'momentum://incrementer'

context = Context()

stream = context.subscribe(STREAM)

try:
    while context.is_subscribed(STREAM):
        string = context.receive_string(stream)
        if string is not None:
            print('Received:', string)
finally:
    context.term()