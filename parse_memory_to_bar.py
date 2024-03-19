import matplotlib.pyplot as plt

file_path = 'memory_message_asy.txt' 

labels = []
values = []
time = 0

with open(file_path, 'r') as file:
    for line in file:
        if line != "\n":
            label, value = line.strip().split()
            labels.append(time)
            time += 0.002
            values.append(float(value))

plt.bar(labels, values)
plt.ylim(76, 77)

plt.title('memory_message_asy')
plt.xlabel('time(s)')
plt.ylabel('memory(%)')
plt.savefig('memory_message_asy.png')
