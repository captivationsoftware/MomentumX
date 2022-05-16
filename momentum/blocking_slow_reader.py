from ctypes import *
import sys
import time

from momentum import Context

def on_read(data, data_length, ts, iteration):
    print(data[:data_length])
    time.sleep(1)

STREAM = b'momentum://incrementer'

context = Context()

context.subscribe(STREAM)

try:
    while (context.is_subscribed(STREAM)):
        context.read(STREAM, on_read)
except KeyboardInterrupt:
    context.term()

