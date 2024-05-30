import time
def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} executed in {end_time - start_time} seconds")
        return result
    return wrapper


import logging
logging.basicConfig(level=logging.INFO)
def log_execution(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
        result = func(*args, **kwargs)
        logging.info(f"{func.__name__} returned: {result}")
        return result
    return wrapper



from functools import lru_cache
@lru_cache(maxsize=None)
def fibonacci(n):
    if n < 2:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)



def type_check(*arg_types):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for arg, expected_type in zip(args, arg_types):
                if not isinstance(arg, expected_type):
                    raise TypeError(f"Argument {arg} is not of type {expected_type}")
            return func(*args, **kwargs)
        return wrapper
    return decorator