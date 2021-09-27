import functools
import hashlib
import os
import pathlib
import pickle
import queue
import threading
import zmq

CONTEXT_ATTR = '__momentum_context__'

NAME_CONTEXT_ATTR = 'name'
PRODUCER_CONTEXT_ATTR = 'producers'
CONSUMER_CONTEXT_ATTR = 'consumers'

__all__ = ["processor", "input", "output"]

class IO:
	def __init__(self):
		self.inputs = {}
		self.outputs = {}

def processor(
	name, 
	*function,
	data_path=os.getenv('MOMENTUM_DATA_PATH', '/dev/shm/momentum'), 
):
	# grab positional function argument
	function = next(iter(function), None)

	if function is None:
		return functools.partial(processor, name, data_path=data_path)

	if not os.path.isdir(data_path):
		pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)

	unwrapped_function = _unwrap(function)

	@functools.wraps(function)
	def composed(*args, **kwargs):
		
		# If the wrapped function is a class, instantiate the instance
		if type(composed) != type(unwrapped_function):
			instance = unwrapped_function(*args, **kwargs)
		else:
			instance = None

		context = getattr(unwrapped_function, CONTEXT_ATTR)

		consumer_contexts = context.get(CONSUMER_CONTEXT_ATTR)
		producer_contexts = context.get(PRODUCER_CONTEXT_ATTR)

		threads = []

		try:
			zmq_context = zmq.Context()

			if len(consumer_contexts) > 0:
				consumer_q_by_handler = {}
				
				consumer_sock = zmq_context.socket(zmq.SUB)
				for consumer_stream, _, handler in consumer_contexts:
					consumer_q_by_handler[handler] = queue.Queue()

					consumer_sock.setsockopt(zmq.SUBSCRIBE, consumer_stream.encode('utf8'))
					consumer_sock.connect(f'ipc://@{consumer_stream}.sock')
		
				def consume():
					while True:
						stream = consumer_sock.recv_string()
						data = consumer_sock.recv()
						for consumer_stream, _, handler in consumer_contexts:
							if consumer_stream == stream:
								consumer_q_by_handler[handler].put_nowait((stream, data))
								
				consumer_thread = threading.Thread(target=consume)
				threads.append(consumer_thread)
		except:
			pass

		# Start a thread to emit messages from handler(s) on <producer_streams>
		if len(producer_contexts) > 0:
			producer_q = queue.Queue()

			producer_sock = zmq_context.socket(zmq.PUB)
			for producer_stream, _, _ in producer_contexts:
				producer_sock.bind(f'ipc://@{producer_stream}.sock')

			
		invocables = {}

		for stream, transformer, handler in consumer_contexts:
			invocables[handler] = invocables.get(handler, IO())
			invocables[handler].inputs[stream] = transformer

		for stream, transformer, handler in producer_contexts:
			invocables[handler] = invocables.get(handler, IO())
			invocables[handler].outputs[stream] = transformer

		def writer(stream, serializer):
			def write(data):
				producer_sock.send_string(stream, zmq.SNDMORE)
				if isinstance(data, (bytes, bytearray, memoryview)):
					producer_sock.send(data, copy=False)
				else:
					if serializer:
						data = serializer(data)

					producer_sock.send(data.encode())
				
			return write

		def reader(deserializer, data):
			def read():
				if deserializer:
					return deserializer(data)
				else:
					return data
			return read


		def work(handler, invocables):
			io = invocables[handler]
			is_producer = len(invocables[handler].outputs) > 0
			is_consumer = len(invocables[handler].inputs) > 0

			while True:
				inputs = {}
				if is_consumer:
					if consumer_q_by_handler[handler].qsize() > 10:
						consumer_q_by_handler[handler].empty()
						
					stream, data = consumer_q_by_handler[handler].get()

				for other_stream, deserializer in io.inputs.items():
					if stream == other_stream and data is not None:
						inputs[stream] = reader(deserializer, data)
					else:
						inputs[other_stream] = None

				outputs = {}
				for other_stream, serializer in io.outputs.items():
					outputs[other_stream] = writer(other_stream, serializer) 
				
				params = {  }
				if instance and not hasattr(handler, '__self__'):
					handler(instance, **inputs, **outputs)
				else:
					handler(**inputs, **outputs)
		
		for handler in invocables.keys():
			worker_thread = threading.Thread(target=work, args=(handler, invocables))
			threads.append(worker_thread)

		try:
			[ t.start() for t in threads ]
			[ t.join() for t in threads ]
		except:
			#cleanup
			pass


	context = _get_context(function)
	context[NAME_CONTEXT_ATTR] = name

	for value in dir(function):
		member = getattr(function, value)
		if hasattr(member, CONTEXT_ATTR):
			member_context = getattr(member, CONTEXT_ATTR)

			for consumer in member_context.get(CONSUMER_CONTEXT_ATTR):
				if consumer not in context[CONSUMER_CONTEXT_ATTR]:
					context[CONSUMER_CONTEXT_ATTR].append(consumer)
			
			for producer in member_context.get(PRODUCER_CONTEXT_ATTR):
				if producer not in context[PRODUCER_CONTEXT_ATTR]:
					context[PRODUCER_CONTEXT_ATTR].append(producer)

	# set the context on the underlying function
	setattr(unwrapped_function, CONTEXT_ATTR, context)

	return composed
	

def input(stream, *function, deserializer=None):
	# grab positional function argument
	function = next(iter(function), None)

	if function is None:
		return functools.partial(input, stream, deserializer=deserializer)

	@functools.wraps(function)
	def composed(*args, **kwargs):
		function(*(args + (stream,)), **kwargs)

	context = _get_context(function)
	context[CONSUMER_CONTEXT_ATTR].append((stream, deserializer, _unwrap(function)))

	# set the modified context on the wrapped function
	setattr(composed, CONTEXT_ATTR, context)

	return composed

def output(stream, *function, serializer=None):
	# grab positional function argument
	function = next(iter(function), None)

	if function is None:
		return functools.partial(output, stream, serializer=serializer)

	@functools.wraps(function)
	def composed(*args, **kwargs):
		function(*(args + (stream,)), **kwargs)

	context = _get_context(function)
	context[PRODUCER_CONTEXT_ATTR].append((stream, serializer, _unwrap(function)))

	# set the modified context on the wrapped function
	setattr(composed, CONTEXT_ATTR, context)

	return composed


def _get_context(function):
	if hasattr(function, '__self__'):
		function = getattr(function, '__self__')

	if hasattr(function, CONTEXT_ATTR):
		context = getattr(function, CONTEXT_ATTR)
	else:
		context = { }
		context[NAME_CONTEXT_ATTR] = None,
		context[PRODUCER_CONTEXT_ATTR] = []
		context[CONSUMER_CONTEXT_ATTR] = []
	
	return context


def _unwrap(function):
	while hasattr(function, '__wrapped__'):
		function = getattr(function, '__wrapped__')
	return function