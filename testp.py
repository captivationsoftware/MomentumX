from ctypes import *

momentum = cdll.LoadLibrary("libmomentum/libmomentum.so")

try:
    momentum.producer()
except (KeyboardInterrupt, SystemExit):
    sys.exit()
