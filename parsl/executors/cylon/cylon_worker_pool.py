#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import uuid

from mpi4py import MPI

from parsl.executors.cylon import DEFAULT_LOGGER
from parsl.executors.cylon.manager import Manager
from parsl.executors.cylon.worker import Worker


def start_file_logger(filename, rank, name=DEFAULT_LOGGER, level=logging.DEBUG, format_string=None):
    """
    Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  " \
                        "%(message)s".format(rank)
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


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
            logger = start_file_logger('{}/manager.mpi_rank_{}.log'.format(args.logdir, rank),
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
        else:
            logger = start_file_logger('{}/worker.mpi_rank_{}.log'.format(args.logdir, rank),
                                       rank,
                                       level=logging.DEBUG if args.debug is True else logging.INFO)
            worker = Worker(worker_topic_str, comm, local_comm)
            worker.start()
    except Exception as e:
        logger.critical("mpi_worker_pool exiting from an exception")
        logger.exception("Caught error: {}".format(e))
        raise e
    else:
        logger.info("mpi_worker_pool exiting")
        logger.debug("Finalizing MPI Comm")
        local_comm.Free()
        comm.Abort()
