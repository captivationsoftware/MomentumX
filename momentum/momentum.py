import ctypes
import glob
import os
from enum import Enum

try:
    import momentum.ext
    libmomentum = glob.glob(f'{os.path.dirname(momentum.ext.__file__)}/libmomentum*.so', recursive=True)[0] 
except:
    raise Exception("Could not locate momentum extension module")

# C API mappings

class STREAM_BUFFER_STATE(ctypes.Structure):
    _fields_ = [
        ("buffer_id", ctypes.c_uint16),
        ("buffer_size", ctypes.c_size_t),
        ("buffer_count", ctypes.c_size_t),
        ("data_size", ctypes.c_size_t),
        ("data_timestamp", ctypes.c_uint64),
        ("iteration", ctypes.c_uint64),
    ]

lib = ctypes.cdll.LoadLibrary(libmomentum)

class LogLevel(Enum):
    DEBUG = ctypes.c_uint8.in_dll(lib, "MOMENTUM_LOG_LEVEL_DEBUG")
    INFO = ctypes.c_uint8.in_dll(lib, "MOMENTUM_LOG_LEVEL_INFO")
    WARNING = ctypes.c_uint8.in_dll(lib, "MOMENTUM_LOG_LEVEL_WARNING")
    ERROR = ctypes.c_uint8.in_dll(lib, "MOMENTUM_LOG_LEVEL_ERROR")
    
lib.momentum_context.argtypes = (ctypes.c_uint8,)
lib.momentum_context.restype = ctypes.c_void_p

lib.momentum_log_level.argtypes = (ctypes.c_void_p, ctypes.c_uint8,)
lib.momentum_log_level.restype = None

lib.momentum_term.argtypes = (ctypes.c_void_p,)
lib.momentum_term.restype = ctypes.c_bool

lib.momentum_destroy.argtypes = (ctypes.c_void_p,)
lib.momentum_destroy.restype = ctypes.c_bool

lib.momentum_is_terminated.argtypes = (ctypes.c_void_p,)
lib.momentum_is_terminated.restype = ctypes.c_bool

lib.momentum_is_subscribed.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_is_subscribed.restype = ctypes.c_bool

lib.momentum_subscribe.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_subscribe.restype = ctypes.c_void_p

lib.momentum_unsubscribe.argtypes = (ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_unsubscribe.restype = None

lib.momentum_subscriber_count.argtypes = (ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_subscriber_count.restype = ctypes.c_size_t

lib.momentum_stream.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_bool,)
lib.momentum_stream.restype = ctypes.c_void_p

lib.momentum_stream_next.argtypes = (ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_stream_next.restype = ctypes.c_void_p

lib.momentum_stream_send.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_stream_send.restype = ctypes.c_bool

lib.momentum_stream_receive.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint64,)
lib.momentum_stream_receive.restype = ctypes.c_void_p

lib.momentum_get_by_buffer_id.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint16,)
lib.momentum_get_by_buffer_id.restype = ctypes.c_void_p

lib.momentum_stream_flush.argtypes = (ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_stream_flush.restype = None

lib.momentum_stream_release.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p)
lib.momentum_stream_release.restype = ctypes.c_bool

lib.momentum_is_stream_sync.argtypes = (ctypes.c_void_p, ctypes.c_void_p,)
lib.momentum_is_stream_sync.restype = ctypes.c_bool

lib.momentum_data_address.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint16,)
lib.momentum_data_address.restype = ctypes.POINTER(ctypes.c_char)

class Context:

    # Constructor
    def __init__(self, log_level=LogLevel.WARNING):
        self._context = lib.momentum_context(log_level.value)
        self._last_data_timestamp_by_stream = {}
        
    # Destructor
    def __del__(self):
        self.term()
        self.destroy()

    # Context manager enter (to enable "with" syntax)
    def __enter__(self):
        return self

    # Context manager exit 
    def __exit__(self, *args):
        self.term()

    def log_level(self, log_level):
        lib.momentum_log_level(self._context, log_level.value)

    def is_terminated(self):
        return bool(lib.momentum_is_terminated(self._context))

    def is_subscribed(self, stream_name):
        return bool(
            lib.momentum_is_subscribed(
                self._context, 
                stream_name.encode() if isinstance(stream_name, str) else stream_name
            )
        )

    def subscribe(self, stream_name):
        stream_name = stream_name.encode() if isinstance(stream_name, str) else stream_name
        return lib.momentum_subscribe(self._context, stream_name)
    
    def unsubscribe(self, stream):
        return bool(
            lib.momentum_unsubscribe(
                self._context, 
                stream
            )
        )

    def subscriber_count(self, stream):
        return lib.momentum_subscriber_count(self._context, stream)


    def stream(self, stream_name, buffer_size, buffer_count=0, sync=False):
        stream_name = stream_name.encode() if isinstance(stream_name, str) else stream_name

        return (
            lib.momentum_stream(self._context, stream_name, buffer_size, buffer_count, sync)
        )

    def is_stream_sync(self, stream):
        return bool(
            lib.momentum_is_stream_sync(self._context, stream)
        )
    

    def next(self, stream):
        pointer = lib.momentum_stream_next(
            self._context, 
            stream
        )

        if pointer is not None:
            return StreamBufferState(self._context, stream, pointer)
        else: 
            return None
        

    def send(self, stream, stream_buffer_state):
        return bool(
            lib.momentum_stream_send(
                self._context, 
                stream, 
                stream_buffer_state._pointer
            )
        )

    def receive(self, stream, minimum_timestamp=0):
        if stream not in self._last_data_timestamp_by_stream:
            self._last_data_timestamp_by_stream[stream] = 0

        if minimum_timestamp == 0:
            minimum_timestamp = self._last_data_timestamp_by_stream[stream] + 1

        pointer = lib.momentum_stream_receive(
            self._context,
            stream,
            minimum_timestamp
        )

        if pointer is not None:
            stream_buffer_state = StreamBufferState(self._context, stream, pointer)

            if stream_buffer_state.data_timestamp > self._last_data_timestamp_by_stream[stream]:
                self._last_data_timestamp_by_stream[stream] = stream_buffer_state.data_timestamp
    
            return stream_buffer_state
        else:
            return None


    def get_by_buffer_id(self, stream, buffer_id):
        pointer = lib.momentum_get_by_buffer_id(self._context, stream, buffer_id)
        if pointer is not None:
            return StreamBufferState(self._context, stream, pointer)
        else:
            return None


    def release(self, stream, stream_buffer_state):
        return bool(
            lib.momentum_stream_release(
                self._context, 
                stream,
                stream_buffer_state._pointer,
            )
        )

    def flush(self, stream):
        lib.momentum_stream_flush(
            self._context, 
            stream
        )

    def receive_string(self, stream):
        stream_buffer_state = self.receive(stream)

        if stream_buffer_state is not None:
            string = bytes(stream_buffer_state.contents[0:stream_buffer_state.data_size]).decode()
            self.release(stream, stream_buffer_state)
            return string
        else:
            return None


    def send_string(self, stream, data, timestamp=0):
        if not isinstance(data, (str, bytes, bytearray)):
            raise Exception("Data must be instance of string or bytes-like")

        stream_buffer_state = self.next(stream)

        if stream_buffer_state is None:
            return False

        data = data.encode() if isinstance(data, str) else data
        for i in range(len(data)):
            stream_buffer_state.contents[i] = data[i] 

        stream_buffer_state.data_size = len(data)

        return self.send(stream, stream_buffer_state)


    def term(self):
        return bool(lib.momentum_term(self._context))

    def destroy(self):
        return_value = lib.momentum_destroy(self._context) 
        self._context = None
        return bool(return_value)

class StreamBufferState:
    def __init__(self, context, stream, pointer):
        if pointer is None:
            raise Exception("Null StreamBufferState pointer")

        try:
            self._context = context
            self._stream = stream
            self._pointer = pointer
            self._stream_buffer_state = STREAM_BUFFER_STATE.from_address(self._pointer)

            self._buffer_id = self._stream_buffer_state.buffer_id
            self._buffer_size = self._stream_buffer_state.buffer_size
            self._buffer_count = self._stream_buffer_state.buffer_count
            self._data_size = self._stream_buffer_state.data_size
            self._data_timestamp = self._stream_buffer_state.data_timestamp
            self._iteration = self._stream_buffer_state.iteration

            self._data_address = lib.momentum_data_address(self._context, self._stream, self._buffer_id)
            self._contents = ctypes.cast(
                self._data_address, 
                ctypes.POINTER(ctypes.c_char * self._buffer_size)
            ).contents
        except:
            raise Exception("StreamBufferState instantiation failed")
    
    @property
    def buffer_id(self):
        return self._buffer_id

    @property
    def buffer_size(self):
        return self._buffer_size

    @property
    def buffer_count(self):
        return self._buffer_count

    @property
    def data_size(self):
        return self._data_size

    @data_size.setter
    def data_size(self, value=0):
        self._data_size = value
        self._stream_buffer_state.data_size = value

    @property
    def data_timestamp(self):
        return self._stream_buffer_state.data_timestamp

    @data_timestamp.setter
    def data_timestamp(self, value=0):
        self._data_timestamp = value
        self._stream_buffer_state.data_timestamp = value

    @property 
    def iteration(self):
        return self._iteration

    @property 
    def data_address(self):
        return self.data_address

    @property
    def contents(self):
        return self._contents
