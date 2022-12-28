import momentumx as mx

STREAM = "mx://incrementer"

context = mx.Context()
stream = context.stream(STREAM, 100, 10, True)
for n in range(500000):
    if stream.send_string(str(n)):
        print(f"Sent: {n}")
