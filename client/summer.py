import json
from momentum_client import momentum

def process(data, **kwargs):
    print(sum(data))
        
momentum(process, deserializer=json.loads)
