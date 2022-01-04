#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import os
import pickle
import platform
import queue
import sys
import threading
import time
import uuid

import zmq
from mpi4py import MPI

from parsl.app.cylon import CylonDistResult, _cylon_comm_key, _cylon_local_comm_key
from parsl.app.errors import RemoteExceptionWrapper
from parsl.serialize import unpack_apply_message, serialize
from parsl.version import VERSION as PARSL_VERSION

from queue import Empty

RESULT_TAG = 10
TASK_REQUEST_TAG = 11

LOOP_SLOWDOWN = 0.0  # in seconds

HEARTBEAT_CODE = (2 ** 32) - 1


def _create_final_result(tid, results):
    # success = True
    data = []
    for res in results:
        # if an error has occurred, raise it
        if not res[0]:
            result_package = {'task_id': tid, 'exception': res[1]}
            return pickle.dumps(result_package)
        # success = success and res[0]
        data.append(res[1])

    result = CylonDistResult(data, True)
    result_package = {'task_id': tid, 'result': serialize(result)}
    return pickle.dumps(result_package)


def _create_exception_result(tid, exception):
    result_package = {'task_id': tid, 'exception': serialize(exception)}
    return pickle.dumps(result_package)


class Manager(object):
    """ Orchestrates the flow of tasks and results to and from the workers

    1. Queue up task requests from workers
    2. Make batched requests from to the interchange for tasks
    3. Receive and distribute tasks to workers
    4. Act as a proxy to the Interchange for results.
    """

    def __init__(self,
                 comm, rank,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 uid=None,
                 address="127.0.0.1",
                 task_bcast_port_range=(56000, 57000),
                 worker_topic=""):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        heartbeat_threshold : int
             Number of seconds since the last message from the interchange after which the worker
             assumes that the interchange is lost and the manager shuts down. Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the interchange

        """
        self.uid = uid

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)
        self.task_incoming.connect(task_q_url)

        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        self.result_outgoing.setsockopt(zmq.LINGER, 0)
        self.result_outgoing.connect(result_q_url)

        self.task_bcasting = self.context.socket(zmq.PUB)
        # self.task_bcasting.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        self.task_bcasting.setsockopt(zmq.LINGER, 0)
        self.task_bcasting_port = \
            self.task_bcasting.bind_to_random_port(f"tcp://{address}",
                                                   min_port=task_bcast_port_range[0],
                                                   max_port=task_bcast_port_range[1])
        self.task_bcasting_url = f"tcp://{address}:{self.task_bcasting_port}"
        self.worker_topic = worker_topic
        # self.task_bcasting.connect(self.task_bcasting_url)

        logger.info(f"Manager connected (task bcast url {self.task_bcasting_url})")
        self.max_queue_size = max_queue_size + comm.size

        # Creating larger queues to avoid queues blocking
        # These can be updated after queue limits are better understood
        self.pending_task_queue = queue.Queue()
        self.pending_result_queue = queue.Queue()
        self.workers_ready = False

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.comm = comm
        self.rank = rank
        self.worker_pool_sz = comm.size - 1

    def create_reg_message(self):
        """
        Creates a registration message to identify the worker to the interchange
        """
        msg = {'parsl_v': PARSL_VERSION,
               'python_v': "{}.{}.{}".format(sys.version_info.major,
                                             sys.version_info.minor,
                                             sys.version_info.micro),
               'os': platform.system(),
               'hostname': platform.node(),
               'dir': os.getcwd(),
               'prefetch_capacity': 0,
               'worker_count': (self.comm.size - 1),
               'max_capacity': (self.comm.size - 1) + 0,  # (+prefetch)
               'reg_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               }
        b_msg = json.dumps(msg).encode('utf-8')
        return b_msg

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (HEARTBEAT_CODE).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat : {}".format(r))

    def set_kill_event(self):
        # put a dummy msg to the pending task queue, so that its consumer would die
        self.pending_task_queue.put(None)
        self._kill_event.set()

    def pull_tasks(self, kill_event):
        """ Pulls tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK PULL THREAD] starting")
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        # Send a registration message
        msg = self.create_reg_message()
        logger.debug("Sending registration message: {}".format(msg))
        self.task_incoming.send(msg)
        last_beat = time.time()
        last_interchange_contact = time.time()
        task_recv_counter = 0

        poll_timer = 1

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            # ready_worker_count = self.ready_worker_queue.qsize()
            ready_worker_count = self.worker_pool_sz  # if self.workers_ready else 0
            pending_task_count = self.pending_task_queue.qsize()

            logger.debug(
                "[TASK_PULL_THREAD] ready workers:{}, pending tasks:{}".format(ready_worker_count,
                                                                               pending_task_count))

            if time.time() > last_beat + self.heartbeat_period:
                self.heartbeat()
                last_beat = time.time()

            if pending_task_count < self.max_queue_size and ready_worker_count > 0:
                logger.debug("[TASK_PULL_THREAD] Requesting tasks: {}".format(ready_worker_count))
                msg = ((ready_worker_count).to_bytes(4, "little"))
                self.task_incoming.send(msg)

            socks = dict(poller.poll(timeout=poll_timer))

            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                _, pkl_msg = self.task_incoming.recv_multipart()
                tasks = pickle.loads(pkl_msg)
                last_interchange_contact = time.time()

                if tasks == 'STOP':
                    logger.critical("[TASK_PULL_THREAD] Received stop request")
                    self.set_kill_event()
                    break

                elif tasks == HEARTBEAT_CODE:
                    logger.debug("Got heartbeat from interchange")

                else:
                    # Reset timer on receiving message
                    poll_timer = 1
                    task_recv_counter += len(tasks)
                    logger.debug("[TASK_PULL_THREAD] Got tasks: {} of {}".format(
                        [t['task_id'] for t in tasks],
                        task_recv_counter))
                    for task in tasks:
                        self.pending_task_queue.put(task)
            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                poll_timer = min(self.heartbeat_period * 1000, poll_timer * 2)

                # Only check if no messages were received.
                if time.time() > last_interchange_contact + self.heartbeat_threshold:
                    logger.critical("[TASK_PULL_THREAD] Missing contact with interchange beyond "
                                    "heartbeat_threshold")
                    self.set_kill_event()
                    logger.critical("[TASK_PULL_THREAD] Exiting")
                    break

    def push_results(self, kill_event):
        """
        Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        # We set this timeout so that the thread checks the kill_event and does not
        # block forever on the internal result queue
        timeout = 0.1
        # timer = time.time()
        logger.debug("[RESULT_PUSH_THREAD] Starting thread")

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            try:
                items = []
                while not self.pending_result_queue.empty():
                    r = self.pending_result_queue.get(block=True)
                    items.append(r)
                if items:
                    self.result_outgoing.send_multipart(items)

            except queue.Empty:
                logger.debug(
                    "[RESULT_PUSH_THREAD] No results to send in past {}seconds".format(timeout))

            except Exception as e:
                logger.exception("[RESULT_PUSH_THREAD] Got an exception : {}".format(e))

        logger.critical("[RESULT_PUSH_THREAD] Exiting")

    def start(self):
        """
        Start the Manager process.
        """
        logger.debug("Manager broadcasting its task bcast address")
        self.comm.bcast(self.task_bcasting_url, root=0)

        self.comm.Barrier()
        logger.debug("Manager synced with workers")
        self.workers_ready = True

        # TODO use a better approach here. Use multiprocessing here, may be?
        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(target=self.pull_tasks,
                                                    args=(self._kill_event,))
        self._result_pusher_thread = threading.Thread(target=self.push_results,
                                                      args=(self._kill_event,))
        self._task_puller_thread.start()
        self._result_pusher_thread.start()

        start = time.time()
        task_get_timeout = 1

        logger.info("Loop start")
        while not self._kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            logger.debug(f"workers available {self.worker_pool_sz} ")

            try:
                logger.debug(f"Waiting for tasks for {task_get_timeout}s")
                task = self.pending_task_queue.get(timeout=task_get_timeout)

                if task is None:
                    logger.critical("None task received. Exiting loop")
                    break

                # Reset timer on receiving message
                task_get_timeout = 1

                tid = task['task_id']
                try:
                    logger.debug(f"Broadcasting task to workers: {task}")
                    self.task_bcasting.send_multipart([self.worker_topic.encode('utf8'),
                                                       pickle.dumps(task)])
                    comm.barrier()

                    # wait for all results
                    results = comm.gather((None, None), root=0)
                    logger.debug(f"Results received {results}")
                    assert len(results) == self.comm.size

                    logger.debug(f"results: {results}")
                    self.pending_result_queue.put(_create_final_result(tid, results[1:]))

                except Exception as e_:
                    self.pending_result_queue.put(_create_exception_result(tid, e_))
            except Empty:
                logger.debug("No tasks received for execution")
                task_get_timeout = min(self.heartbeat_period, task_get_timeout * 2)

        self._task_puller_thread.join()
        self._result_pusher_thread.join()

        self.task_incoming.close()
        self.result_outgoing.close()
        self.task_bcasting.close()
        self.context.term()

        delta = time.time() - start
        logger.info("mpi_worker_pool ran for {} seconds".format(delta))


def execute_task(bufs, comm_, local_comm_):
    """Deserialize the buffer and execute the task.

    Returns the serialized result or exception.
    """
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    kwargs.update({_cylon_comm_key: comm_, _cylon_local_comm_key: local_comm_})

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


def worker(worker_topic, comm: MPI.Comm, rank: int, local_comm: MPI.Comm, local_rank: int):
    logger.info(f"Worker started rank: {rank} local_rank: {local_rank}")

    master_bcast_url = comm.bcast(None, root=0)
    logger.debug(f"Received master bcast url {master_bcast_url}")

    zmq_context = zmq.Context()
    task_bcasting = zmq_context.socket(zmq.SUB)
    task_bcasting.setsockopt(zmq.LINGER, 0)
    task_bcasting.setsockopt_string(zmq.SUBSCRIBE, worker_topic)
    task_bcasting.connect(master_bcast_url)

    poller = zmq.Poller()
    poller.register(task_bcasting, zmq.POLLIN)

    # Sync worker with master
    comm.Barrier()
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
        comm.barrier()

        try:
            result = execute_task(req['buffer'], comm, local_comm)
        except Exception as e:
            result_package = (False, serialize(RemoteExceptionWrapper(*sys.exc_info())))
            logger.debug(
                "No result due to exception: {} with result package {}".format(e, result_package))
        else:
            result_package = (True, serialize(result))
            logger.debug("Result: {}".format(result))

        comm.gather(result_package, root=0)


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(
            rank)

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_stream_logger(name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (sting) : Set to None by default.

    Returns:
         - None
    """
    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-l", "--logdir", default="parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Manager")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("--hb_period", default=30,
                        help="Heartbeat period in seconds. Uses manager default unless set")
    parser.add_argument("--hb_threshold", default=120,
                        help="Heartbeat threshold in seconds. Uses manager default unless set")
    parser.add_argument("-r", "--result_url", required=True,
                        help="REQUIRED: ZMQ url for posting results")
    parser.add_argument("-a", "--address", required=True,
                        help="REQUIRED: Master IP address")

    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    color = int(rank != 0)  # rank 0 --> color 0, else color 1
    key = 0 if rank == 0 else rank - 1
    local_comm = comm.Split(color, key)

    print(f"Starting rank: {rank} color: {color} local_rank: {key}")

    os.makedirs(args.logdir, exist_ok=True)

    worker_topic_str = "0"
    try:
        if rank == 0:
            start_file_logger('{}/manager.mpi_rank_{}.log'.format(args.logdir, rank),
                              rank,
                              level=logging.DEBUG if args.debug is True else logging.INFO)

            logger.info("Python version: {}".format(sys.version))

            manager = Manager(comm, rank,
                              task_q_url=args.task_url,
                              result_q_url=args.result_url,
                              uid=args.uid,
                              heartbeat_threshold=int(args.hb_threshold),
                              heartbeat_period=int(args.hb_period),
                              address=args.address,
                              task_bcast_port_range=(56000, 57000),
                              worker_topic=worker_topic_str)
            manager.start()
            logger.debug("Finalizing MPI Comm")
            local_comm.Free()
            comm.Abort()
        else:
            start_file_logger('{}/worker.mpi_rank_{}.log'.format(args.logdir, rank),
                              rank,
                              level=logging.DEBUG if args.debug is True else logging.INFO)
            worker(worker_topic_str, comm, rank, local_comm, key)
    except Exception as e:
        logger.critical("mpi_worker_pool exiting from an exception")
        logger.exception("Caught error: {}".format(e))
        raise
    else:
        logger.info("mpi_worker_pool exiting")
        print("MPI_WORKER_POOL exiting.")
