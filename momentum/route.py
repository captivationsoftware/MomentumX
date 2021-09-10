import os 
import pathlib
import hashlib

from momentum.stream import list_streams
from momentum.processor import list_processors

def create_route(route, env):
    stream_names = [ s.get('name') for s in list_streams(env) ]
    processor_names = [ p.get('name') for p in list_processors(env) ]

    existing_routes = list_routes(env)

    if route in existing_routes:
        raise OSError(f'Route "{route}" already exists')

    source, dest = route.split(':')
    is_stream_source = source in stream_names
    is_stream_dest = dest in stream_names
    is_processor_source = source in processor_names
    is_processor_dest = dest in processor_names

    if not is_stream_source and not is_processor_source:
        raise OSError(f"Unrecognized route source [{source}]")
    elif not is_stream_dest and not is_processor_dest:
        raise OSError(f"Unrecognized route destination [{dest}]")
    elif is_stream_source and is_stream_dest:
        raise OSError(f"Cannot route stream to stream [{route}]")
    elif is_processor_source and is_processor_dest:
        raise OSError(f"Cannot route processor to processor [{route}]")

    route_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'routes')
    pathlib.Path(route_path).mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.join(route_path, route)).touch()

    # create an additional buffer for this stream if the stream is the source (i.e. new reader)
    if is_stream_source:
        buffer_path = os.path.join(env['MOMENTUM_DATA_PATH'], source)
        num_buffers = len(os.listdir(buffer_path))
        pathlib.Path(os.path.join(buffer_path, str(num_buffers))).touch()

def remove_route(route, env):
    if route not in list_routes(env):
        raise OSError(f'Unrecognized route "{route}"')
    
    source, dest = route.split(':')

    stream_names = [ s.get('name') for s in list_streams(env) ]
    is_stream_source = source in stream_names
    is_stream_dest = dest in stream_names

    # attempt to de-allocate memory for the stream processor
    if is_stream_source:
        buffer_path = os.path.join(env['MOMENTUM_DATA_PATH'], source)
        num_buffers = len(os.listdir(buffer_path))
        os.unlink(os.path.join(buffer_path, str(num_buffers - 1)))

    route_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'routes')
    pathlib.Path(os.path.join(route_path, route)).unlink()


def list_routes(env):

    route_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'routes')
    pathlib.Path(route_path).mkdir(parents=True, exist_ok=True)
    
    routes = []
    for route in sorted(os.listdir(route_path)):
        routes.append(route)

    return routes
