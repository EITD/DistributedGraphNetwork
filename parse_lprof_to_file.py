import sys
import subprocess

def profile_lprof_to_file(profile_file, output_file):
    command = f'python3 -m line_profiler {profile_file}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    with open(output_file, 'w') as f:
        f.write(result.stdout)

if __name__ == "__main__":
    # profile_file = 'worker_old.py.lprof'
    profile_file = 'worker_asy.py.lprof'   
    output_file = "profile_results" + sys.argv[1] + ".txt"

    profile_lprof_to_file(profile_file, output_file)
