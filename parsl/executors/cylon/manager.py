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
from queue import Empty

import zmq

from parsl.app.cylon import CylonDistResult
from parsl.executors.cylon import DEFAULT_LOGGER, HEARTBEAT_CODE, LOOP_SLOWDOWN
from parsl.serialize import serialize
from parsl.version import VERSION as PARSL_VERSION

logger = logging.getLogger(DEFAULT_LOGGER)


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
        global logger
        logger = logging.getLogger(DEFAULT_LOGGER)

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
        # self.task_bcasting.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))DEFAULT_LOGGER
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
                    self.comm.barrier()

                    # wait for all results
                    results = self.comm.gather((None, None), root=0)
                    logger.debug(f"Results received {results}")
                    assert len(results) == self.comm.size

                    logger.debug(f"results: {results}")
                    self.pending_result_queue.put(self.make_final_result(tid, results[1:]))

                except Exception as e_:
                    self.pending_result_queue.put(self.make_exception_result(tid, e_))
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

    def make_final_result(self, tid, results):
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

    def make_exception_result(self, tid, exception):
        result_package = {'task_id': tid, 'exception': serialize(exception)}
        return pickle.dumps(result_package)


class CylonEnvManager(Manager):
    def __init__(self, comm, rank,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 uid=None,
                 address="127.0.0.1",
                 task_bcast_port_range=(56000, 57000),
                 worker_topic=""):
        super().__init__(comm, rank, task_q_url, result_q_url, max_queue_size, heartbeat_threshold,
                         heartbeat_period, uid, address, task_bcast_port_range, worker_topic)
