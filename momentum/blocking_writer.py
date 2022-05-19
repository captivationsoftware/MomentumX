from ctypes import *
import time

from momentum import Context

STREAM = b'momentum://incrementer'

context = Context()
context.debug = True

context.sync = True

i = 0
try:
    while True:
        if context.send_string(STREAM, str(i), timeout=60):
            i += 1
        
except KeyboardInterrupt:
    context.term()