from ctypes import *
import time

import momentumx as mx

STREAM = b'mx://incrementer'

context = mx.Context()

stream = context.subscribe(STREAM)

try:
    while context.is_subscribed(STREAM):
        string = context.receive_string(stream)
        if string is not None:
            print('Received:', string)
finally:
    context.term()