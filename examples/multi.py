import threading
import time

from momentumx import Context, LogLevel


def emitter(cancel):
    context = Context(LogLevel.DEBUG)
    stream = context.stream("emitter", 8, 4, True)

    i = 0
    while i <= 60 and not cancel.is_set():
        if stream.send_string(str(i)):
            i += 1
            time.sleep(0.25)
    cancel.set()


def doubler(cancel):
    context = Context()
    time.sleep(1)
    istream = context.subscribe("emitter")
    ostream = context.stream("doubler", 8, 4, True)
    while context.is_subscribed("emitter") and not cancel.is_set():
        sval = istream.receive_string()
        if sval:
            ival = int(sval.strip())
            while not ostream.send_string(str(ival * 2)):
                pass


def printer(cancel):
    context = Context()
    time.sleep(2)
    stream = context.subscribe("doubler")
    while context.is_subscribed("doubler") and not cancel.is_set():
        val = stream.receive_string()
        if val:
            print(val)

cancel = threading.Event()
t1 = threading.Thread(target=emitter, args=(cancel,))
t2 = threading.Thread(target=doubler, args=(cancel,))
t3 = threading.Thread(target=printer, args=(cancel,))

t1.start()
t2.start()
t3.start()

try:
    while not cancel.wait(0.5):
        pass
finally:
    cancel.set()
    t1.join()
    t2.join()
    t3.join()
