import multiprocessing

def validate_chunks(n):
    if n == 0:
        raise AssertionError('The number of chunks cannot be 0 ')
    elif n <= -2:
        raise AssertionError('The number of chunks should be -1 or > 0')

def get_num_partitions(given_partitions, n):
    if given_partitions == -1:
        return multiprocessing.cpu_count()
    elif given_partitions > n:
        return n
    else:
        return given_partitions

def get_num_cores():
    return multiprocessing.cpu_count()

def wrap(object):
    return object