from asyncio import sleep
from MySocket import MySocket

NUM_PARTITIONS = 2

s = MySocket(myNode=0, port=12346, NUM_PARTITIONS=2)

print(s.serverDict)

input("run?")

for i in range(10):
        s.ask(i, node=i, msg=str((i, i)))

exit(0)