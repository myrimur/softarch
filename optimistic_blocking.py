from builtins import int
from dataclasses import dataclass
from time import sleep
from multiprocessing import Process

from hazelcast.client import HazelcastClient


def optimistic_update_member():
    hz = HazelcastClient()
    map = hz.get_map("map").blocking()
    for k in range(1000):
        if k % 100 == 0:
            print("At: " + str(k))
        while True:
            old_value = map.get(key)
            new_value = Value(old_value.amount + 1)
            sleep(0.01)
            if map.replace_if_same(key, old_value, new_value):
                break


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
        processes.append(Process(target=optimistic_update_member))
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    print("Finished! Result = " + str(map.get(key).amount))
