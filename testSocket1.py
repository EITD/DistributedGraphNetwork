from asyncio import sleep
from MySocket import MySocket

NUM_PARTITIONS = 2

d = {0:("130.229.156.171",12346)}

s = MySocket(myNode=1, port=12345, serverDict=d)


for i in range(10):
        s.ask(i, node=i%NUM_PARTITIONS, msg=str((i, i)))