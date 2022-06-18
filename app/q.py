import queue
import threading
import time
import random

# def q_monitiring_thread():
def monitor_que_obj():
    while True:
        if my_queue.empty():
            print("Empty que")
        else:
            print("que_size: ", my_queue.qsize())
            print(my_queue.get(block=False))
            time.sleep(1)

# Process thr queue
def process_queue():
    
    while True:
        try:
            value = my_queue.get(block=False)
        except queue.Empty:
            return
        else:
            print_multiply(value)
            time.sleep(0.5)

        # function to multiply


def print_multiply(x):
    output_value = []
    for i in range(1, x + 1):
        output_value.append(i * x)
        print(f" \n *** The multiplication result for the {x} is - {output_value}")

# Input variables
input_values = [2, 4, 6, 5,10,3]

# fill the queue
my_queue = queue.Queue()
for x in input_values:
    my_queue.put(x)

x = threading.Thread(target=monitor_que_obj)
x.start()
v = 0
while True:
    if v == 1:
        print("Value Hit")
        time.sleep(5)
        v = 0
    else:
        time.sleep(1)
        v = random.randint(0, 9)
        print("Random generated: ", v)
        my_queue.put(v)
# initializing and starting 3 threads
# MultiThread('First')


