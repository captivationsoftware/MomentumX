import threading
import time

from momentumx import Producer, Consumer


def emitter(cancel: threading.Event):
    stream = Producer("emitter", 8, 4, True, cancel)
    while stream.subscriber_count == 0:
        print("waiting for subscriber(s)")
        if cancel.wait(0.5):
            print("emitter canceled before starting")
            return

    i = 0
    while i <= 60 and not cancel.is_set():
        if stream.send_string(str(i)):
            i += 1
            time.sleep(0.25)
    cancel.set()


def doubler(cancel: threading.Event):
    time.sleep(1)
    istream = Consumer("emitter", cancel)
    ostream = Producer("doubler", 8, 4, True, cancel)

    while ostream.subscriber_count == 0:
        print("waiting for subscriber(s)")
        if cancel.wait(0.5):
            print("doubler canceled before starting")
            return

    while istream.has_next and not cancel.is_set():
        sval = istream.receive_string()
        if sval:
            ival = int(sval.strip())
            assert ostream.send_string(str(ival * 2))


def printer(cancel: threading.Event):
    time.sleep(2)
    stream = Consumer("doubler", cancel)
    while stream.has_next and not cancel.is_set():
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
except KeyboardInterrupt:
    print("received ctrl-c")
finally:
    cancel.set()
    t1.join()
    t2.join()
    t3.join()
