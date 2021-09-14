import os
import pathlib
import click
import re
import yaml

# from momentum.stream import list_streams, create_stream, remove_stream
# from momentum.route import list_routes, create_route, remove_route
# from momentum.processor import list_processors, start_processor, stop_processor, tail_processor_log

import momentum.runtime as runtime

# Environment ############################################################

env = os.environ.copy()

# Loads RC file variables at <path> into env if not already set 
def _load_rc_file_into_env(path):
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
_load_rc_file_into_env(os.path.join(env['PWD'], RC_FILE_NAME))
_load_rc_file_into_env(os.path.join(env['HOME'], RC_FILE_NAME))

# ensure the run path is set
run_path = os.path.abspath(env.get('MOMENTUM_RUN_PATH', '/run/momentum'))
pathlib.Path(run_path).mkdir(parents=True, exist_ok=True)
env['MOMENTUM_RUN_PATH'] = run_path

# ensure the data path is set
data_path = os.path.abspath(env.get('MOMENTUM_DATA_PATH', '/dev/shm/momentum'))
pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)
env['MOMENTUM_DATA_PATH'] = data_path

# ensure the var path is set
var_path = os.path.abspath(env.get('MOMENTUM_VAR_PATH', '/var/lib/momentum'))
pathlib.Path(var_path).mkdir(parents=True, exist_ok=True)
env['MOMENTUM_VAR_PATH'] = var_path

if 'MOMENTUM_LOG_PATH' not in env:
    env['MOMENTUM_LOG_PATH'] = os.path.join(env['MOMENTUM_VAR_PATH'], 'log')
pathlib.Path(env['MOMENTUM_LOG_PATH']).mkdir(parents=True, exist_ok=True)


# CLI entry point ############################################################


@click.group()
def cli():
    pass


@cli.command('install')
@click.argument('processor')
@click.option('-c', '--command', required=True)
@click.option('-f', '--force', is_flag=True)
def install_command(processor, command, force):
    try:
        runtime.install(processor, command, force, env)
    except runtime.DuplicateName as ex:
        print('ERROR:', str(ex), 'Use -f/--force flag to force overwrite.')
    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('uninstall')
@click.argument('processor')
def uninstall_command(processor):
    try:
        runtime.uninstall(processor, env)
    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('processors')
def list_processors_command():
    try:
        output_string = '{:<32}{}'
        print(
            output_string.format(
                'PROCESSOR', 'COMMAND'
            )
        )
        
        for p in runtime.list_processors(env):
            print(
                output_string.format(
                    p.get('processor'), 
                    p.get('command'), 
                )
            )

    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('streams')
def list_streams_command():
    try:
        output_string = '{:<32}{:<16}{:<16}{}'
        print(
            output_string.format(
                'STREAM', 'BUFFER_SIZE', 'BUFFER_COUNT', 'TOTAL_MEMORY'
            )
        )
        
        for s in runtime.list_streams(env):
            print(
                output_string.format(
                    s.get('stream'), 
                    s.get('buffer_size'), 
                    s.get('buffer_count'), 
                    s.get('total_memory')
                )
            )
    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('run')
@click.argument('processor')
@click.option('-d', '--daemon', is_flag=True)
@click.option('-i', '--input', '--inputs', 'input_streams', default='')
@click.option('-o', '--output', '--outputs', 'output_streams', default='')
def run_command(processor, daemon, input_streams, output_streams):
    input_streams = [ x.strip() for x in input_streams.split(',') if x ]
    output_streams = [ x.strip() for x in output_streams.split(',') if x ]

    try:
        runtime.run(processor, daemon, input_streams, output_streams, env)
    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('ps')
@click.argument('processors', nargs=-1)
def ps_command(processors):
    try:
        output_string = '{:<32}{:<16}{:<16}{}'
        print(
            output_string.format(
                'PROCESSOR', 'PID', 'STATE', 'CREATE_TIME'
            )
        )
        
        for status in runtime.ps(list(processors), env):
            print(
                output_string.format(
                    status.get('processor'), 
                    status.get('pid') or '-', 
                    status.get('status') or '-', 
                    status.get('create_time') or '-'
                )
            )
    except Exception as ex:
        print('ERROR:', str(ex))

@cli.command('kill')
@click.argument('processors', nargs=-1, required=True)
def kill_command(processors):
    try:
        runtime.kill(list(processors), env)
    except Exception as ex:
        print('ERROR:', str(ex))


@cli.command('tail')
@click.argument('processors', nargs=-1)
@click.option('-f', '--follow', is_flag=True)
def log_command(processors, follow):
    try:
        runtime.tail(list(processors), follow, env)
    except Exception as ex:
        print('ERROR:', str(ex))


# # Processor commands ##########################################################


# @cli.group('processor')
# def processor_commands():
#     pass

# @processor_commands.command('start')
# @click.argument('processor_name')
# @click.argument('command')
# @click.option('-d', '--daemon', 'daemon', is_flag=True)
# def start_processor_command(processor_name, command, daemon):
#     try:
#         start_processor(processor_name, command, env=env)
#         if not daemon:
#             tail_processor_log([processor_name], follow=True, env=env)
#     except OSError as ex:
#         print('ERROR:', str(ex))

# @processor_commands.command('stop')
# @click.argument('processor_name', nargs=-1, required=True)
# def remove_processor_command(processor_name):
#     try:
#         [ stop_processor(p, env) for p in processor_name ]
#     except OSError as ex:
#         print('ERROR:', str(ex))

# @processor_commands.command('list')
# def list_processors_command():
#     output_string = '{:<24}{:<16}{:<16}{}'
#     print(output_string.format('PROCESSOR_NAME', 'STATUS', 'PID', 'COMMAND'))
    
#     for processor in list_processors(env):
#         print(
#             output_string.format(
#                 processor.get('name'), 
#                 processor.get('status'), 
#                 processor.get('pid') or '-',
#                 processor.get('command') or '-' 
#             )
#         )

# @processor_commands.command('log')
# @click.argument('processor', nargs=-1)
# @click.option('-f', '--follow', 'follow', is_flag=True)
# def log_command(processor, follow):
#     try:
#         tail_processor_log(list(processor), follow=follow, env=env)
#     except OSError as ex:
#         print('ERROR:', str(ex))



# # Stream commands #############################################################


# @cli.group('stream')
# def stream_commands():
#     pass

# @stream_commands.command('create')
# @click.argument('stream_name', nargs=-1, required=True)
# def create_stream_command(stream_name):
#     try:
#         [ create_stream(n, env) for n in stream_name ]
#     except OSError as ex:
#         print('ERROR:', str(ex))

# @stream_commands.command('remove')
# @click.argument('stream_name', nargs=-1, required=True)
# def remove_stream_command(stream_name):
#     try:
#         [ remove_stream(n, env) for n in stream_name ]
#     except OSError as ex:
#         print('ERROR:', str(ex))
    

# @stream_commands.command('list')
# def list_stream_command():
#     output_string = '{:<24}{:<16}{:<16}{}'
#     print(
#         output_string.format(
#             'STREAM_NAME', 'BUFFER_SIZE', 'BUFFER_COUNT', 'TOTAL_MEMORY'
#         )
#     )
    
#     for s in list_streams(env):
#         print(
#             output_string.format(
#                 s.get('name'), 
#                 s.get('buffer_size'), 
#                 s.get('buffer_count'), 
#                 s.get('total_memory')
#             )
#         )


# # Route commands ###############################################################


# @cli.group('route')
# def route_commands():
#     pass

# @route_commands.command('create')
# @click.argument('route', nargs=-1, required=True)
# def create_route_command(route):
#     try:
#         [ create_route(r, env) for r in route ]
#     except OSError as ex:
#         print('ERROR:', str(ex))

# @route_commands.command('remove')
# @click.argument('route', nargs=-1, required=True)
# def remove_route_command(route):
#     try:
#         [ remove_route(r, env) for r in route ]
#     except OSError as ex:
#         print('ERROR:', str(ex))

# @route_commands.command('list')
# def list_routes_command():
#     output_string = '{:<24}'
#     print(output_string.format('ROUTE'))
    
#     for route in list_routes(env):
#         print(output_string.format(route))


# # Application commands #########################################################


# @cli.command('application')
# @click.option('-f', default='momentum.yaml', type=click.Path(exists=True))
# @click.argument('command', type=click.Choice(['start', 'stop']))
# @click.option('-d', '--daemon', 'daemon', is_flag=True)
# def run_application(f, daemon, command):

#     with open(os.path.abspath(os.path.join(env['PWD'], f)), 'r') as conf_yaml:
#         try:
#             conf = yaml.safe_load(conf_yaml)
#         except:
#             print(f"ERROR: Invalid yaml file [{f}]")
#             sys.exit(1)

#     processor_names = []
#     for processor_name, processor_conf in conf.get('processors', {}).items():
#         try:
#             if command == 'start':
#                 processor_names.append(processor_name)
#                 start_processor(processor_name, processor_conf.get('command'), env=env)
#             elif command == 'stop':
#                 stop_processor(processor_name, env=env)
#         except OSError:
#             pass

#     for stream_name, stream_conf in conf.get('streams', {}).items():
#         try:
#             if command == 'start':
#                 create_stream(stream_name, env=env)
#             elif command == 'stop':
#                 remove_stream(stream_name, env=env)
#         except OSError:
#             pass
    
#     for route in conf.get('routes', []):
#         try:
#             if command == 'start':
#                 create_route(route, env=env)
#             elif command == 'stop':
#                 remove_route(route, env=env)
#         except OSError:
#             pass
    
#     if not daemon:
#         tail_processor_log(processor_names, env=env)