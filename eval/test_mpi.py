import parsl
from parsl import python_app
from parsl.config import Config

from parsl.executors import CylonExecutor

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
def test_func(x, comm=None, local_comm=None, **kwargs):
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


doubled_x = test_func(1)
print(doubled_x.result())
