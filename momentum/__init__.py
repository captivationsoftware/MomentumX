import argparse
import os
import pathlib
import click
import hashlib

from momentum.stream import list_streams, create_stream, remove_stream
from momentum.route import list_routes, create_route, remove_route
from momentum.processor import list_processors, create_processor, remove_processor

# Start by first making a copy of the runtime env
env = os.environ.copy()

# Loads RC file variables at <path> into env if not already set 
def load_rc_file_into_env(path):
    if os.path.exists(path):
        with open(path, 'r') as rc:
            for line in rc.readlines():
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    var, val = line.strip().split('=')
                    
                    # Do not add a var that is already set 
                    if var and val and var not in env:
                        env[var] = val

# incorporate rc file overrides into env, starting with closest path outward
RC_FILE_NAME = '.momentumrc'
load_rc_file_into_env(os.path.join(env['PWD'], RC_FILE_NAME))
load_rc_file_into_env(os.path.join(env['HOME'], RC_FILE_NAME))

# ensure the run path is set
run_path = os.path.abspath(env.get('MOMENTUM_RUN_PATH', '/run/momentum'))
pathlib.Path(run_path).mkdir(parents=True, exist_ok=True)
env['MOMENTUM_RUN_PATH'] = run_path

# ensure the data path is set
data_path = os.path.abspath(env.get('MOMENTUM_DATA_PATH', '/dev/shm/momentum'))
pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)
env['MOMENTUM_DATA_PATH'] = data_path

@click.group()
def cli():
    pass

@cli.group('processor')
def processor_commands():
    pass

@cli.group('stream')
def stream_commands():
    pass

@cli.group('route')
def route_commands():
    pass


@stream_commands.command('create')
@click.argument('stream_name', nargs=-1, required=True)
def create_stream_command(stream_name):
    try:
        [ create_stream(n, env) for n in stream_name ]
    except OSError as ex:
        print('ERROR:', str(ex))

@stream_commands.command('remove')
@click.argument('stream_name', nargs=-1, required=True)
def remove_stream_command(stream_name):
    try:
        [ remove_stream(n, env) for n in stream_name ]
    except OSError as ex:
        print('ERROR:', str(ex))
    

@stream_commands.command('list')
def list_stream_command():
    output_string = '{:<24}{:<16}{:<16}{}'
    print(output_string.format('STREAM_NAME', 'BUFFER_SIZE', 'BUFFER_COUNT', 'TOTAL_MEMORY'))
    
    for s in list_streams(env):
        print(output_string.format(s.get('name'), s.get('buffer_size'), s.get('buffer_count'), s.get('total_memory')))

@route_commands.command('create')
@click.argument('route', nargs=-1, required=True)
def create_route_command(route):
    try:
        [ create_route(r, env) for r in route ]
    except OSError as ex:
        print('ERROR:', str(ex))

@route_commands.command('remove')
@click.argument('route', nargs=-1, required=True)
def remove_route_command(route):
    try:
        [ remove_route(r, env) for r in route ]
    except OSError as ex:
        print('ERROR:', str(ex))

@route_commands.command('list')
def list_routes_command():
    output_string = '{:<24}'
    print(output_string.format('ROUTE'))
    
    for route in list_routes(env):
        print(output_string.format(route))
            

@processor_commands.command('create')
@click.argument('processor_name')
@click.argument('command')
def create_processor_command(processor_name, command):
    try:
        create_processor(processor_name, command, env)
    except OSError as ex:
        print('ERROR:', str(ex))

@processor_commands.command('remove')
@click.argument('processor_name', nargs=-1, required=True)
def remove_processor_command(processor_name):
    try:
        [ remove_processor(p, env) for p in processor_name ]
    except OSError as ex:
        print('ERROR:', str(ex))

@processor_commands.command('list')
def list_processors_command():
    output_string = '{:<24}{}'
    print(output_string.format('PROCESSOR_NAME', 'COMMAND'))
    
    for processor in list_processors(env):
        print(
            output_string.format(
                processor.get('name'), 
                processor.get('command'), 
            )
        )


