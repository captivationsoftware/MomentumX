from ctypes import *
import time

momentum = cdll.LoadLibrary("../libmomentum/libmomentum.so")

context = momentum.context()

print(momentum.is_terminated(context))

momentum.term(context)

print(momentum.is_terminated(context))

momentum.destroy(context)