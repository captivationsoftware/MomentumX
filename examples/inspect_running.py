import json
import momentumx as mx
import os

try:
    # Attempt to use tabulate if it's already pip-installed
    from tabulate import tabulate
except ImportError:
    # If not installed, fall back to easily parsable table to print, eg,
    #   python3 examples/inspect_running.py | column -ts '|'
    def tabulate(data, headers):
        lines = [
            " | ".join(headers),
            *[" | ".join([str(item) for item in row]) for row in data],
        ]
        return "\n".join(lines)

f_beg = "mx."
f_end = ".buffer.1"

fnames = os.listdir("/dev/shm")
fnames = filter(lambda f: f.startswith(f_beg), fnames)
fnames = filter(lambda f: f.endswith(f_end), fnames)

for f in fnames:
    stream_name = f[len(f_beg) : -len(f_end)]
    ins = mx.Inspector(stream_name)

    snapshot = json.loads(ins.control_snapshot())[0]
    buffers = snapshot.pop("buffers")

    print("-- Control Block --")
    print(tabulate([[*snapshot.values()]], headers=[*snapshot.keys()]))

    print("-- Buffer State --")
    items = [[*b[0]["buffer_state"].values()] for b in buffers]
    hdrs = [*buffers[0][0]["buffer_state"].keys()]
    print(tabulate(items, headers=hdrs))

    print("-- Buffer Sync --")
    items = [[*b[0]["buffer_sync"][0].values()] for b in buffers]
    hdrs = [*buffers[0][0]["buffer_sync"][0].keys()]
    print(tabulate(items, headers=hdrs))
