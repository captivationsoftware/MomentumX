import momentum 
import json 
import time
import os

MESSAGE = b'\x01' * int(1e7)

@momentum.processor('benchmark_tx')
@momentum.output('benchmark')
def tx(benchmark):
    benchmark(MESSAGE)
    

@momentum.processor('benchmark_rx')
class rx:
    def __init__(self):
        self.before = time.time()
        self.messages = 0
        self.bytes_received = 0

    @momentum.input('benchmark')
    def rx(self, benchmark):
        self.messages += 1    
        self.bytes_received += len(benchmark())

        if self.messages % 100 == 0:
            elapsed = time.time() - self.before
            print("Recvd {:.2f} msgs/sec".format(self.messages / elapsed))
            print("Recvd {:.2f} MB/sec".format(self.bytes_received / elapsed / 1.0e6))


@momentum.processor('benchmark')
class rx_tx:
    def __init__(self):
        self.before = time.time()
        self.messages = 0
        self.bytes_received = 0

    @momentum.output('benchmark')
    def tx(self, benchmark):
        benchmark(MESSAGE)
        

    @momentum.input('benchmark')
    def rx(self, benchmark):
        self.messages += 1    
        self.bytes_received += len(benchmark())

        if self.messages % 100 == 0:
            elapsed = time.time() - self.before
            print("Recvd {:.2f} msgs/sec".format(self.messages / elapsed))
            print("Recvd {:.2f} MB/sec".format(self.bytes_received / elapsed / 1.0e6))

rx_tx()

# if os.fork() == 0:
#     tx()
# else: 
#     rx()
