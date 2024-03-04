import matplotlib.pyplot as plt

def parse_dat_file(filename):
    memory_usage = {}
    with open(filename, 'r') as f:
        for line in f:
            parts = line.split()
            if parts[0] == "FUNC":
                func_name = parts[1].split('.')[-1]
                mem_usage = float(parts[4])
                if func_name in memory_usage:
                    memory_usage[func_name] = max(memory_usage[func_name], mem_usage)
                else:
                    memory_usage[func_name] = mem_usage
    return memory_usage

def plot_memory_usage(memory_dict, output_file):
    function_names = []
    memory_usages = []
    for key, value in memory_dict.items():
        function_names.append(key)
        memory_usages.append(value)   

    plt.figure(figsize=(10, 8))
    plt.barh(function_names, memory_usages, color='skyblue')
    plt.xlabel('Memory Usage')
    plt.title('Memory Consuming Functions')
    plt.gca().invert_yaxis()
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()

if __name__ == "__main__":
    for index in range(4):
        filename ="mprofile" + str(index) + ".dat"
        memory_usage = parse_dat_file(filename)
        plot_memory_usage(memory_usage, "memory_results" + str(index) + ".png")
