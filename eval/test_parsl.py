import parsl
from parsl import python_app

parsl.load()


@python_app
def wait_sleep_double(x):
    import time
    time.sleep(2)  # Sleep for 2 seconds
    return x * 2


@python_app
def wait_sleep_double2(x, fut1, fut2):
    import time
    time.sleep(2)  # Sleep for 2 seconds
    res = x * 2

    time.sleep(3)  # Sleep for 2 seconds
    res += fut1

    time.sleep(3)  # Sleep for 2 seconds
    res += fut2

    return res


# Launch two apps, which will execute in parallel, since they do not have to
# wait on any futures
doubled_x = wait_sleep_double(1)
doubled_y = wait_sleep_double(2)

# The third app depends on the first two:
#    doubled_x   doubled_y     (2 s)
#           \     /
#           doublex_z          (2 s)
doubled_z = wait_sleep_double2(3, doubled_x.result(), doubled_y.result())

# doubled_z will be done in ~4s
print(doubled_z.result())
