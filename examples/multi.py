from ctypes import *
import threading
import time

from cv2 import log

from momentum import Context, LogLevel


def emitter():
    context = Context(log_level=LogLevel.DEBUG)
    stream = context.stream('emitter', 8, 4, True)
    
    i = 1
    while i < 60 :
        if context.send_string(stream, str(i)):
            i += 1
            time.sleep(0.25)


def doubler():
    context = Context()
    time.sleep(1)
    istream = context.subscribe('emitter')
    ostream = context.stream('doubler', 8, 4, True)
    while context.is_subscribed('emitter'):
        sval = context.receive_string(istream)
        if sval:
            ival = int(sval.strip())
            while not context.send_string(ostream, str(ival * 2)):
                pass

def printer():
    context = Context()
    time.sleep(2)
    stream = context.subscribe('doubler')
    while context.is_subscribed('doubler'):
        val = context.receive_string(stream)
        if val:
            print(val)



t1 = threading.Thread(
    target=emitter
)

t2 = threading.Thread(
    target=doubler
)

t3 = threading.Thread(
    target=printer
)

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()