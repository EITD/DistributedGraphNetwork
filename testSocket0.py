from asyncio import sleep
from MySocket import MySocket

NUM_PARTITIONS = 2

d = {1:("130.229.156.171",12345)}

s = MySocket(myNode=0, port=12346, serverDict=d)

for i in range(10):
        s.ask(i, node=i%NUM_PARTITIONS, msg=str((i, i)))