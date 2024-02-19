from asyncio import sleep
from MySocket import MySocket

NUM_PARTITIONS = 2

s = MySocket(myNode=0, port=12346, NUM_PARTITIONS=2)

input("run?")

for i in range(10):
        s.ask(i, node=i%NUM_PARTITIONS, msg=str((i, i)))