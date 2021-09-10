import os 
import socket
import sys

def momentum(proc, serializer=lambda x: x, deserializer=lambda x: x):
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        sock.connect(os.getenv('MOMENTUM_ADDRESS'))
        
        is_consumer = os.getenv('MOMENTUM_INPUT') != ''
        is_producer = os.getenv('MOMENTUM_OUTPUT') != ''

        while True:
            channel = None
            data = None

            if is_consumer:
                byte_length = int.from_bytes(sock.recv(8), byteorder=sys.byteorder)
                if byte_length > 0:
                    message = sock.recv(byte_length)

                    channel, data = message.split(b'|')

                    data = serialize(data)

            result = proc(channel=channel, data=data)

            if is_producer:
                data = serializer(result)
                sock.send(len(data).to_bytes(8, sys.byteorder))
                sock.send(data.encode('utf8'))
    finally:
        try:
            sock.close()
        except:
            pass

