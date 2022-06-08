from ctypes import *
import time

from momentum import Context, LogLevel

STREAM = b'momentum://incrementer'

context = Context()

i = 0
try:
    stream = context.stream(STREAM, 100, 10, True)
    while i < 500000:
        if context.send_string(stream, str(i)):
            print("Sent: ", i)
            i += 1
        
except KeyboardInterrupt:
    context.term()