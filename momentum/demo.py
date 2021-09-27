import momentum 
import json 
# import threading
import time

@momentum.processor('counter')
class JsonCounter:
    def __init__(self):
        self.n = 0

    @momentum.output('n', serializer=json.dumps)
    def producer_method(self, n):
        self.n += 1
        n({ "n": self.n })
        
    @momentum.input('n', deserializer=json.loads)
    def consumer_method(self, n):
        print(n())

JsonCounter()
    

# class TestClassExplicit:

#     def __init__(self):
#         momentum.input('printer', json.loads, self.process_evens)
#         momentum.input('odds', json.loads, self.process_odds)

#     def process_evens(self, printer):
#         print('even x2', printer)

#     def process_odds(self, odds):
#         print('odd:', odds)

# proc = momentum.processor('printer', TestClassExplicit)


# threads = [
#     threading.Thread(target=TestClass),
#     threading.Thread(target=test_func),
#     threading.Thread(target=proc)
# ]

# [ x.start() for x in threads ]