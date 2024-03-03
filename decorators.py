import time

def timeit(func):
    def wrapper(*args, **kwargs):
       start = time.time()
       result = func(*args, **kwargs)
       end = time.time()
      #  with open('execution_time', 'a') as f: 
      #   f.write(f"{func.__name__} took {end - start} seconds to execute\n") 
       print(f"took {end - start} seconds to execute\n")
       return result
    return wrapper 