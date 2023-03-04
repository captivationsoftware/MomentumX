import momentumx as mx
import functools
import os

f_beg = "mx."
f_end = ".buffer.1"

fnames = os.listdir("/dev/shm")
fnames = filter(lambda f: f.startswith(f_beg), fnames)
fnames = filter(lambda f: f.endswith(f_end), fnames)

for f in fnames:
    stream_name = f[len(f_beg) : -len(f_end)]
    ins = mx.Inspector(stream_name)
    ins.check_locks()
    print(ins.control_snapshot(), flush=True)
