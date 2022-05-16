import ctypes
import time

# C API mappings

lib = ctypes.cdll.LoadLibrary("./libmomentum.so")

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

lib.momentum_get_blocking.argtypes = (ctypes.c_void_p,)
lib.momentum_get_blocking.restype = ctypes.c_bool

lib.momentum_set_blocking.argtypes = (ctypes.c_void_p, ctypes.c_bool,)
lib.momentum_set_blocking.restype = None

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

lib.momentum_read.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p,)
lib.momentum_read.restype = ctypes.c_bool

lib.momentum_send_string.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint64,)
lib.momentum_send_string.restype = ctypes.c_bool

lib.momentum_acquire_buffer.argtypes = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,)
lib.momentum_acquire_buffer.restype = ctypes.c_void_p

lib.momentum_release_buffer.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64,)
lib.momentum_release_buffer.restype = ctypes.c_bool

lib.momentum_get_buffer_address.argtypes = (ctypes.c_void_p,)
lib.momentum_get_buffer_address.restype = ctypes.POINTER(ctypes.c_uint8)

lib.momentum_get_buffer_length.argtypes = (ctypes.c_void_p,)
lib.momentum_get_buffer_length.restype = ctypes.c_size_t


class Context:

    # Constructor
    def __init__(self):
        self._context = lib.momentum_context()

        self._wrapped_funcs_by_id = {}

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
    def blocking(self):
        return lib.momentum_get_blocking(self._context)

    @blocking.setter
    def blocking(self, value):
        return lib.momentum_set_blocking(self._context, value)
        
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

    def try_subscribe(self, stream):
        if not self.is_stream_available:
            raise Exception(f'Stream "{stream}" not available')

        if not self.is_subscribed(stream):
            return bool(
                lib.momentum_subscribe(
                    self._context, 
                    stream.encode() if isinstance(stream, str) else stream
                )
            )

        return True

    def subscribe(self, stream):
        if self.is_subscribed(stream):
            return True

        while not self.try_subscribe(stream):
            time.sleep(1)

        return True

    def read(self, stream, func):
        if not self.is_subscribed(stream):
            raise Exception(f'Not subscribed to stream "{stream}"')

        func_id = id(func)

        if func_id not in self._wrapped_funcs_by_id:
            @ctypes.CFUNCTYPE(None, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t, ctypes.c_uint64, ctypes.c_longlong)
            def wrapped_func(data_address, data_length, ts, iteration):
                data_pointer = ctypes.cast(data_address, ctypes.POINTER(ctypes.c_uint8 * data_length))
                func(data_pointer.contents, data_length, ts, iteration)

            self._wrapped_funcs_by_id[func_id] = wrapped_func

        return bool(
            lib.momentum_read(
                self._context,
                stream.encode() if isinstance(stream, str) else stream,
                self._wrapped_funcs_by_id[func_id]
            )
        )


    def send_string(self, stream, data, data_length = -1, ts = 0):
        if (data_length == -1):
            data_length = len(data)

        return bool(
            lib.momentum_send_string(
                self._context, 
                stream.encode() if isinstance(stream, str) else stream,
                data.encode() if isinstance(data, str) else data, 
                data_length, 
                ts
            )
        )

    def acquire_buffer(self, stream, buffer_length):
        return lib.momentum_acquire_buffer(
            self._context, 
            stream.encode() if isinstance(stream, str) else stream,
            buffer_length
        )

    def release_buffer(self, buffer, data_length, ts = 0):
        return lib.momentum_release_buffer(
            self._context, 
            buffer,
            data_length,
            ts
        )

    def term(self):
        return bool(lib.momentum_term(self._context))

    def destroy(self):
        return_value = lib.momentum_destroy(self._context) 
        self._context = None
        return bool(return_value)