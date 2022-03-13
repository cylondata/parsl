import logging
import pickle
import sys

import zmq
from mpi4py import MPI

from parsl.app.cylon import _cylon_comm_key, _cylon_local_comm_key
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.cylon import DEFAULT_LOGGER
from parsl.serialize import unpack_apply_message, serialize

logger = logging.getLogger(DEFAULT_LOGGER)


class Worker:
    def __init__(self, worker_topic, comm: MPI.Comm, local_comm: MPI.Comm):
        global logger
        logger = logging.getLogger(DEFAULT_LOGGER)

        self.comm = comm
        self.local_comm = local_comm
        self.worker_topic = worker_topic

    def start(self):
        logger.info(f"Worker started rank: {self.comm.rank} local_rank: {self.local_comm.rank}")

        master_bcast_url = self.comm.bcast(None, root=0)
        logger.debug(f"Received master bcast url {master_bcast_url}")

        zmq_context = zmq.Context()
        task_bcasting = zmq_context.socket(zmq.SUB)
        task_bcasting.setsockopt(zmq.LINGER, 0)
        task_bcasting.setsockopt_string(zmq.SUBSCRIBE, self.worker_topic)
        task_bcasting.connect(master_bcast_url)

        poller = zmq.Poller()
        poller.register(task_bcasting, zmq.POLLIN)

        # Sync worker with master
        self.comm.Barrier()
        logger.debug("Synced")

        while True:
            socks = dict(poller.poll())
            if task_bcasting in socks and socks[task_bcasting] == zmq.POLLIN:
                logger.debug("received task bcast")
                _, pkl_msg = task_bcasting.recv_multipart()
                req = pickle.loads(pkl_msg)
            else:
                logger.debug("Unrelated task received from master. Retrying...")
                continue

            logger.info("Got req: {}".format(req))
            tid = req['task_id']
            logger.debug("Got task: {}".format(tid))
            self.comm.barrier()

            try:
                result = self.execute_task(req['buffer'])
            except Exception as e:
                result_package = (False, serialize(RemoteExceptionWrapper(*sys.exc_info())))
                logger.debug(
                    "No result due to exception: {} with result package {}".format(e,
                                                                                   result_package))
            else:
                result_package = (True, self.make_result(result))
                logger.debug("Result: {}".format(result))

            self.comm.gather(result_package, root=0)

    def make_result(self, result):
        return serialize(result)

    def execute_task(self, bufs):
        """Deserialize the buffer and execute the task.

        Returns the serialized result or exception.
        """
        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})

        f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

        kwargs.update({_cylon_comm_key: self.comm, _cylon_local_comm_key: self.local_comm})

        fname = getattr(f, '__name__', 'f')
        prefix = "parsl_"
        fname = prefix + "f"
        argname = prefix + "args"
        kwargname = prefix + "kwargs"
        resultname = prefix + "result"

        user_ns.update({fname: f,
                        argname: args,
                        kwargname: kwargs,
                        resultname: resultname})

        code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                               argname, kwargname)

        try:
            logger.debug("[RUNNER] Executing: {0}".format(code))
            exec(code, user_ns, user_ns)
        except Exception as e:
            logger.warning("Caught exception; will raise it: {}".format(e))
            raise e
        else:
            logger.debug("[RUNNER] Result: {0}".format(user_ns.get(resultname)))
            return user_ns.get(resultname)


class CylonEnvWorker(Worker):
    def __init__(self, worker_topic, comm: MPI.Comm, local_comm: MPI.Comm):
        super().__init__(worker_topic, comm, local_comm)

    def make_result(self, result):
        # store result in the local store, and send None
        return super().make_result(result)

    def execute_task(self, bufs):
        return super().execute_task(bufs)




