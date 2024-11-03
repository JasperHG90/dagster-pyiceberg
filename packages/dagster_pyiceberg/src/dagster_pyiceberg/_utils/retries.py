from abc import ABCMeta, abstractmethod
from typing import Tuple

from pyiceberg.table import Table
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)


class PyIcebergOperationException(Exception): ...


class PyIcebergOperationWithRetry(metaclass=ABCMeta):

    def __init__(self, table: Table):
        self.table = table

    @abstractmethod
    def operation(self, *args, **kwargs): ...

    def execute(
        self,
        retries: int,
        exception_types: type[BaseException] | Tuple[type[BaseException]],
        *args,
        **kwargs,
    ):
        try:
            for retry in Retrying(
                stop=stop_after_attempt(retries),
                reraise=True,
                wait=wait_random(0.1, 0.99),
                retry=retry_if_exception_type(exception_types),
            ):
                with retry:
                    try:
                        self.operation(*args, **kwargs)
                        # Reset the attempt number on success
                        retry.retry_state.attempt_number = 1
                    except exception_types as e:
                        # Do not refresh on the final try
                        if retry.retry_state.attempt_number < retries:
                            self.table.refresh()
                        raise e
        except RetryError as e:
            raise PyIcebergOperationException(
                f"Max retries exceeded for class {str(self.__class__)}"
            ) from e
