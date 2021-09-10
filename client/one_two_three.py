import json
from momentum_client import momentum

def process(**kwargs):
    return [ 1, 2, 3]
        
momentum(process, serializer=json.dumps, deserializer=json.loads)
