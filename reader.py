from ctypes import *
import sys
import time

lib = cdll.LoadLibrary("./libmomentum.so")

context = lib.momentum_context()

now = time.time()
bytes_received = 0
messages_received = 0

@CFUNCTYPE(None, c_char_p, c_int)
def handle_message(data, length):
    global now
    global bytes_received
    global messages_received
    
    messages_received += 1
    bytes_received += length
    
    if (messages_received % (length / 100) == 0):
        elapsed = time.time() - now

        print("Recvd {:.2f} msgs/sec".format(messages_received / elapsed))
        print("Recvd {:.2f} MB/sec".format(bytes_received / elapsed / 1.0e6))

lib.momentum_subscribe(context, sys.argv[1].encode(), handle_message)


try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    pass

