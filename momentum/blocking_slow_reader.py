from ctypes import *
import sys
import time
import signal

from momentum import Context
context = Context()

def on_read(string, ts, iteration):
    print(string)
    time.sleep(1)

STREAM = b'momentum://incrementer'

context.subscribe(STREAM)

try:
    while (context.is_subscribed(STREAM)):
        context.receive_string(STREAM, on_read)
except KeyboardInterrupt:
    context.term()

