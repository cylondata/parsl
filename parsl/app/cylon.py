from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Optional, Union, List, Literal

import typeguard

from parsl import DataFlowKernel, DataFlowKernelLoader
from parsl.app.python import PythonApp, timeout
from parsl.executors import CylonExecutor
from parsl.serialize import deserialize


class CylonDistResult(object):
    """
    Container for keeping serialized results from each worker
    """

    def __init__(self, data, success: bool):
        """

        Parameters
        ----------
        data: list ob byte buffers
            serialized data
        success: bool
            success/ failure
        """
        self.data_ = data
        self.success_ = success

    @property
    def size(self):
        return len(self.data_)

    @property
    def is_ok(self):
        return self.success_

    def __getitem__(self, index):
        return deserialize(self.data_[index])

    def __str__(self) -> str:
        return '; '.join([self[i] for i in range(self.size)])


class CylonApp(PythonApp):
    def __call__(self, *args, **kwargs):
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        # check if all executors are cylon executors
        for label, ex in dfk.executors.items():
            if label != '_parsl_internal' and not isinstance(ex, CylonExecutor):
                raise ValueError("CylonApp only supports CylonExecutor")

        walltime = invocation_kwargs.get('walltime')
        if walltime is not None:
            func = timeout(self.func, walltime)
        else:
            func = self.func

        app_fut = dfk.submit(func, app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs,
                             join=self.join)

        return app_fut


@typeguard.typechecked
def cylon_app(function=None,
              data_flow_kernel: Optional[DataFlowKernel] = None,
              cache: bool = False,
              executors: Union[List[str], Literal['all']] = 'all',
              ignore_for_cache: Optional[List[str]] = None,
              join: bool = False):
    """Decorator function for making python apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@python_app`` if using all defaults or ``@python_app(walltime=120)``. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    join : bool
        If True, this app will be a join app: the decorated python code must return a Future
        (rather than a regular value), and and the corresponding AppFuture will complete when
        that inner future completes.
    ignore_for_cache: list
    """

    def decorator(func):
        def wrapper(f):
            return CylonApp(f,
                            data_flow_kernel=data_flow_kernel,
                            cache=cache,
                            executors=executors,
                            ignore_for_cache=ignore_for_cache,
                            join=join)

        return wrapper(func)

    if function is not None:
        return decorator(function)
    return decorator
