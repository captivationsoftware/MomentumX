import os 
import pathlib


def create_stream(name, env):
    stream_path = os.path.join(env['MOMENTUM_DATA_PATH'], name)

    if name in [ s.get('name') for s in list_streams(env) ]:
        raise OSError(f'Name "{name}" is already in use by a stream')

    from momentum.processor import list_processors
    if name in [ p.get('name') for p in list_processors(env) ]:
        raise OSError(f'Name "{name}" is already in use by a processor')

    os.mkdir(stream_path)

    # initialize with a single buffer
    pathlib.Path(os.path.join(stream_path, '0')).touch()


def remove_stream(stream_name, env):
    stream_path = os.path.join(env['MOMENTUM_DATA_PATH'], stream_name)
    
    if os.path.exists(stream_path):
        for buffer in os.listdir(stream_path):
            os.unlink(os.path.join(stream_path, buffer))

        os.rmdir(stream_path)                

    # Clean up and routes that were using this stream
    from momentum.route import list_routes, remove_route
    for route in list_routes(env):
        if stream_name in route.split(':'):
            remove_route(route, env)

def list_streams(env):
    data_path = env['MOMENTUM_DATA_PATH']
    
    streams = []
    for stream in sorted(os.listdir(data_path)):
        buffer_size = 0
        buffer_count = 0
        total_memory = 0

        for buffer in os.listdir((os.path.join(data_path, stream))):
            buffer_size = os.path.getsize(os.path.join(data_path, stream, buffer))
            total_memory += buffer_size
            buffer_count += 1
        
        streams.append({ "name": stream, "buffer_size": buffer_size, "buffer_count": buffer_count, "total_memory": total_memory })

    return streams
        
            