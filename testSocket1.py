from asyncio import sleep
from MySocket import MySocket

NUM_PARTITIONS = 2

s = MySocket(myNode=1, port=12345, NUM_PARTITIONS=2)

input("run?")

for i in range(10):
        s.ask(i, node=i%NUM_PARTITIONS, msg=str((i, i)))

exit(0)