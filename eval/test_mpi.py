import parsl
from parsl import python_app
from parsl.config import Config

from parsl.executors import WorkQueueExecutor
from parsl.executors import ExtremeScaleExecutor
from parsl.executors import CylonExecutor
from parsl.launchers import MpiRunLauncher
from parsl.providers import LocalProvider

# config = Config(
#     executors=[
#         WorkQueueExecutor(
#             label="cylon_test",
#             provider=LocalProvider(
#                 launcher=MpiRunLauncher(),
#             ),
#         )
#     ],
# )
config = Config(
    executors=[
        CylonExecutor(
            label="cylon_test",
            # provider=LocalProvider(
            #     launcher=MpiRunLauncher(),
            # ),
            ranks_per_node=4,
            worker_debug=True,
            heartbeat_threshold=30
        )
    ],
)

parsl.load(config=config)


@python_app
def wait_sleep_double(x, **kwargs):
    comm = kwargs['comm']
    local_comm = kwargs['local_comm']
    rank = comm.rank
    world_sz = comm.size

    local_rank = local_comm.rank
    local_world_sz = local_comm.size

    print(f"Starting rank: {rank} world_size: {world_sz} local_rank {local_rank} local_world_sz "
          f"{local_world_sz}")

    import time
    time.sleep(x + rank)  # Sleep for 2 seconds

    out = local_comm.allgather(local_rank * 4)  # all gather in the local comm

    return f"Starting rank: {rank}  {world_sz} {out}"


@python_app
def wait_sleep_double2(x, fut1, fut2, **kwargs):
    from mpi4py import MPI
    import time

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(f"Starting rank1: {rank}", comm.Get_size())

    # import time
    time.sleep(2)  # Sleep for 2 seconds
    res = x * 2

    time.sleep(3)  # Sleep for 2 seconds
    res += fut1

    # time.sleep(3)  # Sleep for 2 seconds
    res += fut2

    return res


# Launch two apps, which will execute in parallel, since they do not have to
# wait on any futures
# doubled_x = wait_sleep_double(1, parsl_resource_specification={'cores': 12, 'memory': 1000, 'disk': 1000})
doubled_x = wait_sleep_double(1)
# doubled_y = wait_sleep_double(2, parsl_resource_specification={'cores': 3, 'memory': 1000, 'disk': 1000})

# The third app depends on the first two:
#    doubled_x   doubled_y     (2 s)
#           \     /
#           doublex_z          (2 s)
# doubled_z = wait_sleep_double2(3, doubled_x.result(), doubled_y.result(), parsl_resource_specification={'cores': 4, 'memory': 1000, 'disk': 1000})

# doubled_z will be done in ~4s
print(doubled_x.result())
