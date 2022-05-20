import ctypes
import time
import glob
import sys
import os

try:
    import momentum.ext
    libmomentum = glob.glob(f'{os.path.dirname(momentum.ext.__file__)}/libmomentum*.so', recursive=True)[0] 
except:
    raise Exception("Could not locate momentum extension module")

# C API mappings

lib = ctypes.cdll.LoadLibrary(libmomentum)

lib.momentum_context.argtypes = ()
lib.momentum_context_restype = ctypes.c_void_p

lib.momentum_get_debug.argtypes = (ctypes.c_void_p,)
lib.momentum_get_debug.restype = ctypes.c_bool

lib.momentum_set_debug.argtypes = (ctypes.c_void_p, ctypes.c_bool,)
lib.momentum_set_debug.restype = None

lib.momentum_get_min_buffers.argtypes = (ctypes.c_void_p,)
lib.momentum_get_min_buffers.restype = ctypes.c_size_t

lib.momentum_set_min_buffers.argtypes = (ctypes.c_void_p, ctypes.c_size_t,)
lib.momentum_set_min_buffers.restype = None

lib.momentum_get_max_buffers.argtypes = (ctypes.c_void_p,)
lib.momentum_get_max_buffers.restype = ctypes.c_size_t

lib.momentum_set_max_buffers.argtypes = (ctypes.c_void_p, ctypes.c_size_t,)
lib.momentum_set_max_buffers.restype = None

lib.momentum_get_sync.argtypes = (ctypes.c_void_p,)
lib.momentum_get_sync.restype = ctypes.c_bool

lib.momentum_set_sync.argtypes = (ctypes.c_void_p, ctypes.c_bool,)
lib.momentum_set_sync.restype = None

lib.momentum_term.argtypes = (ctypes.c_void_p,)
lib.momentum_term_restype = ctypes.c_bool

lib.momentum_destroy.argtypes = (ctypes.c_void_p,)
lib.momentum_destroy_restype = ctypes.c_bool

lib.momentum_is_terminated.argtypes = (ctypes.c_void_p,)
lib.momentum_is_terminated_restype = ctypes.c_bool

lib.momentum_is_stream_available.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_is_stream_available_restype = ctypes.c_bool

lib.momentum_is_subscribed.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_is_subscribed_restype = ctypes.c_bool

lib.momentum_subscribe.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_subscribe_restype = ctypes.c_bool

lib.momentum_unsubscribe.argtypes = (ctypes.c_void_p, ctypes.c_char_p,)
lib.momentum_unsubscribe_restype = ctypes.c_bool

lib.momentum_next_buffer.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,)
lib.momentum_next_buffer.restype = ctypes.c_void_p

lib.momentum_send_buffer.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64,)
lib.momentum_send_buffer.restype = ctypes.c_bool

lib.momentum_receive_buffer.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p,)
lib.momentum_receive_buffer.restype = ctypes.c_bool

lib.momentum_get_buffer_address.argtypes = (ctypes.c_void_p,)
lib.momentum_get_buffer_address.restype = ctypes.POINTER(ctypes.c_uint8)

lib.momentum_get_buffer_length.argtypes = (ctypes.c_void_p,)
lib.momentum_get_buffer_length.restype = ctypes.c_size_t


class Context:

    # Constructor
    def __init__(self):
        self._context = lib.momentum_context()
        self._wrapped_receive_buffer_by_id = {}
        self._wrapped_receive_string_by_id = {}
        
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

    # Properties
    
    @property
    def debug(self):
        return lib.momentum_get_debug(self._context)

    @debug.setter
    def debug(self, value):
        return lib.momentum_set_debug(self._context, value)
        
    @property
    def sync(self):
        return lib.momentum_get_sync(self._context)

    @sync.setter
    def sync(self, value):
        return lib.momentum_set_sync(self._context, value)
        
    @property
    def min_buffers(self):
        return lib.momentum_get_min_buffers(self._context)

    @min_buffers.setter
    def min_buffers(self, value):
        return lib.momentum_set_min_buffers(self._context, value)
    
    @property
    def max_buffers(self):
        return lib.momentum_get_max_buffers(self._context)

    @max_buffers.setter
    def max_buffers(self, value):
        return lib.momentum_set_max_buffers(self._context, value)

    def is_terminated(self):
        return bool(lib.momentum_is_terminated(self._context))

    def is_stream_available(self, stream):
        return bool(
            lib.momentum_is_stream_available(
                self._context, 
                stream.encode() if isinstance(stream, str) else stream
            )
        )
    
    def is_subscribed(self, stream):
        return bool(
            lib.momentum_is_subscribed(
                self._context, 
                stream.encode() if isinstance(stream, str) else stream
            )
        )

    def unsubscribe(self, stream):
        return bool(
            lib.momentum_unsubscribe(
                self._context, 
                stream.encode() if isinstance(stream, str) else stream
            )
        )

    def subscribe(self, stream, timeout=0, retry_interval=0.1):
        if not self.is_stream_available(stream):
            raise Exception(f'Stream "{stream}" not available')

        stream = stream.encode() if isinstance(stream, str) else stream

        retry_interval = max(0.001, retry_interval)
        attempts = max(1, int(timeout / retry_interval))

        for _ in range(attempts):
            if bool(
                lib.momentum_subscribe(
                    self._context,
                    stream
                )
            ):
                return True
            else:
                time.sleep(retry_interval)    
        
        return False

    def next_buffer(self, stream, buffer_length, timeout=0, retry_interval=1e-6):
        retry_interval = max(0.001, retry_interval)
        attempts = max(1, int(timeout / retry_interval))

        for _ in range(attempts):
            pointer = lib.momentum_next_buffer(
                self._context, 
                stream.encode() if isinstance(stream, str) else stream,
                int(buffer_length)
            )

            if pointer is not None:
                return Buffer(pointer)
            else:
                time.sleep(retry_interval)    
        
        return None        

    def receive_buffer(self, stream, func, timeout=0, retry_interval=1e-6):
        if not self.is_subscribed(stream):
            raise Exception(f'Not subscribed to stream "{stream}"')

        stream = stream.encode() if isinstance(stream, str) else stream

        func_id = id(func)

        if func_id not in self._wrapped_receive_buffer_by_id:
            @ctypes.CFUNCTYPE(None, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t, ctypes.c_uint64, ctypes.c_uint64)
            def wrapped_func(buffer_pointer, data_length, ts, iteration):
                try:
                    func(Buffer(buffer_pointer), data_length, ts, iteration)
                except KeyboardInterrupt:
                    self.term()

            self._wrapped_receive_buffer_by_id[func_id] = wrapped_func

        retry_interval = max(0.001, retry_interval)
        attempts = max(1, int(timeout / retry_interval))

        for _ in range(attempts):
            if bool(
                lib.momentum_receive_buffer(
                    self._context,
                    stream,
                    self._wrapped_receive_buffer_by_id[func_id]
                )
            ):
                return True
            else:
                time.sleep(retry_interval)    
        
        return False


    def receive_string(self, stream, func, timeout=0, retry_interval=1e-6):
        func_id = id(func)

        if func_id not in self._wrapped_receive_string_by_id:
    
            def wrapped_func(buffer, data_length, ts, iteration):
                string = buffer.read(data_length)
                func(string, ts, iteration)
    
            self._wrapped_receive_string_by_id[func_id] = wrapped_func
        
        return self.receive_buffer(stream, self._wrapped_receive_string_by_id[func_id], timeout, retry_interval)


    def send_buffer(self, buffer, data_length, ts = 0, timeout=0, retry_interval=1e-6):
        retry_interval = max(0.001, retry_interval)
        attempts = max(1, int(timeout / retry_interval))

        for _ in range(attempts):
            if bool(
                lib.momentum_send_buffer(
                    self._context, 
                    buffer._pointer,
                    data_length,
                    ts
                )
            ):
                return True
            else:
                time.sleep(retry_interval)    
        
        return False


    def send_string(self, stream, data, ts=0, timeout=0, retry_interval=1e-6):
        if not isinstance(data, (str, bytes, bytearray)):
            raise Exception("Data must be instance of string or bytes-like")


        data_length = len(data)

        buffer = self.next_buffer(stream, data_length)
        
        buffer.write(data.encode() if isinstance(data, str) else data, data_length)        

        return self.send_buffer(buffer, data_length, ts, timeout, retry_interval)


    def term(self):
        return bool(lib.momentum_term(self._context))


    def destroy(self):
        return_value = lib.momentum_destroy(self._context) 
        self._context = None
        return bool(return_value)

    def Stream(self, stream):
        return Stream(self, stream)


class Buffer:

    def __init__(self, pointer):
        if pointer is None:
            raise Exception("Null buffer pointer")

        try:
            self._pointer = pointer
            self._data_address = lib.momentum_get_buffer_address(pointer)
            self._length = lib.momentum_get_buffer_length(pointer)
            self._memory = ctypes.cast(self._data_address, ctypes.POINTER(ctypes.c_uint8 * self._length))
        except:
            raise Exception("Buffer instantiation failed")

    @property
    def raw(self):
        return self._memory.contents

    @property
    def length(self):
        return self._length

