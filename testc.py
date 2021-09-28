from ctypes import *

momentum = cdll.LoadLibrary("libmomentum/libmomentum.so")

momentum.consumer()
