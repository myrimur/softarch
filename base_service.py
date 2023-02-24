from concurrent import futures
from abc import ABC, abstractmethod
from signal import signal, SIGTERM
import logging

import grpc
from grpc import ServicerContext
from google.protobuf.empty_pb2 import Empty

from messaging_pb2 import (
    PostRequest,
    GetResponse,
)

from messaging_pb2_grpc import (
    BaseServicer,
    add_BaseServicer_to_server,
)

messages = {}
logging.basicConfig(level=logging.INFO)


class BaseService(BaseServicer, ABC):
    def __init__(self, addr: str | bytes):
        super().__init__()
        self._addr = addr

    @abstractmethod
    def _get(self, request: Empty) -> GetResponse:
        ...

    @abstractmethod
    def _post(self, request: PostRequest) -> Empty:
        ...

    def get(self, request: Empty, context: ServicerContext = None) -> GetResponse:
        logging.info(f'{self.__class__.__name__}.get request: {request}')
        resp = self._get(request)
        logging.info(f'{self.__class__.__name__}.get response: {resp}')
        return resp

    def post(self, request: PostRequest, context: ServicerContext = None) -> Empty:
        logging.info(f'{self.__class__.__name__}.post request: {request}')
        resp = self._post(request)
        logging.info(f'{self.__class__.__name__}.post response: {resp}')
        return resp

    def run(self) -> None:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        add_BaseServicer_to_server(self, server)
        server.add_insecure_port(self._addr)
        server.start()
        logging.info(f'{self.__class__.__name__} running on {self._addr}')

        def handle_sigterm(*_):
            all_rpcs_done_event = server.stop(30)
            all_rpcs_done_event.wait(30)

        signal(SIGTERM, handle_sigterm)
        server.wait_for_termination()
