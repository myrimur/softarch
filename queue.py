from multiprocessing import Process

from hazelcast.client import HazelcastClient


def producer():
    hz = HazelcastClient()
    queue = hz.get_queue("queue").blocking()
    for i in range(1000):
        queue.put(i)
        if i % 100 == 0:
            print("Produced: " + str(i))


def consumer():
    hz = HazelcastClient()
    queue = hz.get_queue("queue").blocking()
    while True:
        item = queue.take()
        if item % 100 == 0:
            print("Consumed: " + str(item))
        if item == 999:
            queue.put(item)  # poison pill
            break


if __name__ == '__main__':
    hz = HazelcastClient()
    queue = hz.get_queue("queue").blocking()
    queue.clear()

    processes = []
    processes.append(Process(target=producer))
    for i in range(2):
        processes.append(Process(target=consumer))
    for p in processes:
        p.start()
    for p in processes:
        p.join()
