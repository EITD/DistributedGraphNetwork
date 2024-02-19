
import random


k = 3
deltas = [5,9,19]

kNew = k - 1
newDeltas = [i // deltas[0] for i in deltas[1:]]
newDeltasList = [newDeltas.copy() for _ in range(deltas[0])]

for i in range(len(newDeltas)):
        s = sum(newDeltasList[j][i] for j in range(len(newDeltasList)))
        
        if s == deltas[i + 1]:
                continue
        
        remaining = deltas[i + 1] - s
        for _ in range(remaining):
                idx = random.randint(0, len(newDeltasList) - 1)
                newDeltasList[idx][i] += 1

print(newDeltasList)

