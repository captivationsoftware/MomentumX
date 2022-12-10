from ctypes import *
import time

import momentumx as mx

STREAM = b'mx://incrementer'

context = mx.Context()

i = 0
try:
    stream = context.stream(STREAM, 100, 10, True)
    while i < 500000:
        if context.send_string(stream, str(i)):
            print("Sent: ", i)
            i += 1
        
except KeyboardInterrupt:
    context.term()