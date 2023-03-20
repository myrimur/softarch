from builtins import int
from dataclasses import dataclass
from time import sleep
from multiprocessing import Process

from hazelcast.client import HazelcastClient


def racy_update_member():
    hz = HazelcastClient()
    map = hz.get_map("map").blocking()
    for k in range(1000):
        if k % 100 == 0:
            print("At: " + str(k))
        value = map.get(key)
        sleep(0.01)
        value.amount += 1
        map.put(key, value)


@dataclass
class Value:
    amount: int = 0


if __name__ == '__main__':
    hz = HazelcastClient()
    map = hz.get_map("map").blocking()
    key = "1"
    map.put(key, Value())
    print("Starting")

    processes = []
    for i in range(3):
        processes.append(Process(target=racy_update_member))
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    print("Finished! Result = " + str(map.get(key).amount))
