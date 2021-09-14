import datetime
import os
import pathlib
import psutil
import random
import re
import subprocess
import threading
import time

PROCESSOR_NAME_MAX_LENGTH = STREAM_NAME_MAX_LENGTH = 24
PROCESSOR_NAME_REGEX = STREAM_NAME_REGEX = r'^[a-z_]+$'

class InvalidName(Exception):
    pass

class DuplicateName(Exception):
    pass

class UnrecognizedName(Exception):
    pass

def install(processor, command, force, env=None):
    _assert_valid_processor_name(processor, env)
    
    processor_file_path = _processor_file_path(processor, env)
    if os.path.exists(processor_file_path):
        if not force:
            raise DuplicateName(
                f'Processor with name "{processor}" already installed.'
            )
        else:
            uninstall(processor, env)


    with open(processor_file_path, 'w') as processor_file:
        processor_file.write(command)

def uninstall(processor, env=None):
    processor_file_path = _processor_file_path(processor, env)

    if os.path.exists(processor_file_path):
        os.unlink(processor_file_path)
        
        # do any other cleanup 

def list_processors(env=None):
    installed_processor_path = os.path.join(env['MOMENTUM_VAR_PATH'], 'installed')
    processors = []
    for processor in sorted(os.listdir(installed_processor_path)):
        command_file_path = os.path.join(installed_processor_path, processor)
        with open(command_file_path, 'r') as command_file:
            command = command_file.read().strip()

        processors.append({ "processor": processor, "command": command })
    
    return processors

def list_streams(env=None):
    streams = []
    for stream in sorted(os.listdir(env['MOMENTUM_DATA_PATH'])):
        buffer_size = 0
        buffer_count = 0
        total_memory = 0

        for buffer in os.listdir((os.path.join(env['MOMENTUM_DATA_PATH'], stream))):
            buffer_size = os.path.getsize(os.path.join(env['MOMENTUM_DATA_PATH'], stream, buffer))
            total_memory += buffer_size
            buffer_count += 1
        
        streams.append({ 
            "stream": stream, 
            "buffer_size": buffer_size, 
            "buffer_count": buffer_count, 
            "total_memory": total_memory 
        })

    return streams
    
def run(processor, daemon, input_streams, output_streams, env=None):
    command = next(
        ( p.get('command') for p in list_processors(env) if p.get('processor') == processor ),
        None
    )

    if not command:
        raise UnrecognizedName(f'Processor with name "{processor}" not installed')

    [ _assert_valid_stream_name(stream, env) for stream in input_streams + output_streams if stream ]

    stream_paths = []
    added_buffer_paths = []

    for output_stream in output_streams:
        stream_path = os.path.join(env['MOMENTUM_DATA_PATH'], output_stream)
        pathlib.Path(stream_path).mkdir(parents=True, exist_ok=True)
        stream_paths.append(stream_path)

        # add two new buffers to initialize with
        num_buffers = len(os.listdir(stream_path))

        buffer_size = 0 if num_buffers == 0 else os.path.getsize(os.path.join(stream_path, '0'))
        for i in range(num_buffers, num_buffers + 2):
            buffer_path = os.path.join(stream_path, str(i))

            # if buffers already exist, match size
            if buffer_size > 0:
                with open(buffer_path, 'wb') as buffer:
                    buffer.seek(buffer_size - 1)
                    buffer.write(b'\0')
            else: 
                pathlib.Path(buffer_path).touch()

            added_buffer_paths.append(buffer_path)

    process = subprocess.Popen(
        command.split(),
        stdout=open(os.path.join(env['MOMENTUM_LOG_PATH'], f'{processor}.log'), 'w'),
        stderr=subprocess.STDOUT, 
        env=env
    )

    pidfile_path = _pidfile_path(processor, env)

    with open(pidfile_path, 'w') as f:
        f.write(str(process.pid))

    if os.fork() == 0:
        psutil.wait_procs([psutil.Process(pid=process.pid)])
        os.unlink(pidfile_path)

        for buffer_path in added_buffer_paths:
            try:
                print('TODO: file lock check')
                os.unlink(buffer_path)
            except:
                pass

        for stream_path in stream_paths:
            if len(os.listdir(stream_path)) == 0:
                os.rmdir(stream_path)
    else:
        if not daemon:
            tail([processor], follow=True, env=env)
        

def kill(processor, env):
    pid = _get_pid(processor, env)

    if pid:
        psutil.Process(pid = pid).kill()

def ps(processors=[], env=None):
    stats = []

    if processors == []:
        processors = os.listdir(env['MOMENTUM_RUN_PATH'])

    for processor in processors:
        state = {
            "processor": processor,
            "pid": None,
            "status": None,
            "create_time": None
        }
       
        try:
            state['pid'] = _get_pid(processor, env)

            p = psutil.Process(pid = state['pid'])
            with p.oneshot():
                state['status'] = p.status()
                state['create_time'] = datetime.datetime.fromtimestamp(p.create_time())
        except:
            pass

        stats.append(state)

    return stats

def tail(processors, follow, env):
    
    def iterate(processor, color_code):
        def log(line):
            output_string = f'\033[{{}}m{{:<{PROCESSOR_NAME_MAX_LENGTH}}}| \033[0m{{}}'
            print(output_string.format(color_code, processor, line.strip()))

        log_path = os.path.join(env['MOMENTUM_LOG_PATH'], f'{processor}.log')

        if os.path.exists(log_path):
            with open(log_path, 'r') as logfile:
                for line in logfile.readlines():
                    log(line)
                
                while follow:
                    # Check for pid and zombie status to determine if we should loop
                    state = next(iter(ps([processor], env)), {})
                    
                    pid = state.get('pid', None)
                    status = state.get('status', None)

                    line = logfile.readline()
                
                    if line and line.endswith('\n'):
                        log(line)

                    if not pid or status == psutil.STATUS_ZOMBIE:
                        break
                    else:
                        time.sleep(0.1)

    color_codes = [ 33, 34, 35, 36, 94, 95, 96 ]
    random.shuffle(color_codes)

    if processors == []:
        processors = [ 
            x.replace('.log', '') 
            for x in os.listdir(env['MOMENTUM_LOG_PATH']) 
        ]

    threads = [ 
        threading.Thread(
            target=iterate(processor, color_codes[index])
        ) for index, processor in enumerate(processors)
    ]     

    [ thread.start() for thread in threads ]   
        
    [ thread.join() for thread in threads ]

##### Utilities #####

def _assert_valid_processor_name(processor, env):
    if len(processor) > PROCESSOR_NAME_MAX_LENGTH:
        raise InvalidName(
            f"Processor names must not exceed {PROCESSOR_NAME_MAX_LENGTH} characters."
        )

    if not re.search(PROCESSOR_NAME_REGEX, processor):
        raise InvalidName(
            "Processor names must only contain lowercase letters, numbers, and _ (underscore) character."
        )

def _assert_valid_stream_name(stream, env):
    if len(stream) > STREAM_NAME_MAX_LENGTH:
        raise InvalidName(
            f"Stream names must not exceed {STREAM_NAME_MAX_LENGTH} characters."
        )

    if not re.search(STREAM_NAME_REGEX, stream):
        raise InvalidName(
            "Stream names must only contain lowercase letters, numbers, and _ (underscore) character."
        )

def _processor_file_path(processor, env):
    return os.path.join(env['MOMENTUM_VAR_PATH'], 'installed', processor)

def _pidfile_path(processor, env):
    return os.path.join(env['MOMENTUM_RUN_PATH'], processor)


def _get_pid(processor, env):
    try:
        pidfile_path = _pidfile_path(processor, env)
        if os.path.exists(pidfile_path):
            with open(pidfile_path, 'r') as pidfile:
                pid = pidfile.read().strip()
                if pid.isdigit():
                    return int(pid)
    except: 
        pass

    return None