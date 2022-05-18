from ctypes import *

from momentum import Context

STREAM = b'momentum://incrementer'

context = Context()
context.debug = True

context.sync = True

i = 0
try:
    while True:
        context.send_string(STREAM, str(i))
        i += 1
except KeyboardInterrupt:
    context.term()