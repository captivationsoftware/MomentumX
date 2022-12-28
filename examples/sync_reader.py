import momentumx as mx

STREAM = "mx://incrementer"

context = mx.Context()
stream = context.subscribe(STREAM)
while context.is_subscribed(STREAM):
    string = stream.receive_string()
    if string:
        print("Received:", string)
