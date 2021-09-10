import os
import pathlib

def create_processor(name, command, env):
    if name in [ p.get('name') for p in list_processors(env) ]:
        raise OSError(f'Name "{name}" is already in use by a processor')

    from momentum.stream import list_streams

    if name in [ s.get('name') for s in list_streams(env) ]:
        raise OSError(f'Name "{name}" is already in use by a stream')

    processor_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'processors')
    pathlib.Path(processor_path).mkdir(exist_ok=True, parents=True)

    with open(os.path.join(processor_path, name), 'w') as f:
        f.write(command)


def remove_processor(processor_name, env):
    processor_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'processors', processor_name)
    
    if not os.path.exists(processor_path):
        raise OSError(f'Unrecognized processor "{processor_name}"')
        
    os.unlink(processor_path)

    # Clean up any routes that were using this processor
    from momentum.route import list_routes, remove_route
    for route in list_routes(env):
        if processor_name in route.split(':'):
            remove_route(route, env)

def list_processors(env):
    processor_path = os.path.join(env['MOMENTUM_RUN_PATH'], 'processors')
    pathlib.Path(processor_path).mkdir(parents=True, exist_ok=True)
    
    processors = []
    for processor in sorted(os.listdir(processor_path)):
        with open(os.path.join(processor_path, processor), 'r') as f:
            processors.append({ "name": processor, "command": f.read() })

    return processors

# parser = argparse.ArgumentParser(prog='Momentum CLI')

# subparsers = parser.add_subparsers()

# run_parser = subparsers.add_parser('run')
# run_parser.add_argument('--exec', type=str, help='Path to executable to run')
# run_parser.add_argument('--input', default='', type=str, help='Comma-delimited input channel(s) to listen on')
# run_parser.add_argument('--output', default='', type=str, help='Comma-delimited output channel(s) to emit on')

# args = parser.parse_args()

# if 'exec' in args:
#     pid = str(os.getpid())
    
#     input_channels = [ x for x in args.input.split(',') if x != '' ]
#     output_channels = [ x for x in args.output.split(',') if x != '' ]

#     env['MOMENTUM_INPUT'] = ','.join(input_channels)
#     env['MOMENTUM_OUTPUT'] = ','.join(output_channels)

#     for output_channel in output_channels:
#         output_channel_data_path = f'{data_path}/{output_channel}'
#         process_output_channel_data_path = os.path.join(output_channel_data_path, pid)
#         pathlib.Path(process_output_channel_data_path).mkdir(parents=True, exist_ok=True)

#         for node in range(0,2):
#             output_buffer_file_path = os.path.join(process_output_channel_data_path, str(node))
#             with open(output_buffer_file_path, 'wb+') as buffer_file:
#                 pass
#                 buffer_file.write(b'\x00' * max_data_length)
#                 buffer_file.flush()

#     try:
#         env["MOMENTUM_ADDRESS"] = os.path.join(run_path, f'{pid}.sock')

#         server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
#         server_sock.bind(env["MOMENTUM_ADDRESS"])

#         server_sock.listen(1)

#         process = subprocess.Popen(
#             args.exec.split(), 
#             env=env
#         )

#         client, _ = server_sock.accept()
#         with client:
#             while True:
#                 # handle output from the momentum client
#                 byte_length = int.from_bytes(client.recv(8), sys.byteorder)
#                 data = client.recv(byte_length)

#                 # notify (eventually write to shared memory and notify that)
#                 for output_channel in output_channels:
#                     socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

#                     message = f'{output_channel}|{data}'.encode('utf8')

#                     output_sock = get_sock(output_channel)
#                     output_sock.send(len(message))
#                     output_sock.send(message)

#     except socket.error:
#         raise
#     except:
#         try:
#             process.kill()
#         except:
#             pass
#     finally:
#         try:
#             server_sock.close()
#             os.unlink(env["MOMENTUM_ADDRESS"])
#         except:
#             pass

#         # cleanup
#         for output_channel in output_channels:
#             output_channel_data_path = os.path.join(data_path, output_channel)
#             process_output_channel_data_path = os.path.join(output_channel_data_path, pid)
            
#             try:
#                 # Remove all buffer files in this process' output channels
#                 for f in os.listdir(process_output_channel_data_path):
#                     os.remove(os.path.join(process_output_channel_data_path, f))

#                 # Remove the process output channel directory
#                 os.rmdir(process_output_channel_data_path)

#                 # If all processes are finished for this output channel, delete the output channel folder as well
#                 if not os.listdir(output_channel_data_path):
#                     os.rmdir(output_channel_data_path)
#             except:
#                 pass