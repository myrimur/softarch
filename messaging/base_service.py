from concurrent import futures
from abc import ABC, abstractmethod
from signal import signal, SIGTERM
import logging
from hazelcast.client import HazelcastClient
import grpc
from grpc import ServicerContext
from google.protobuf.empty_pb2 import Empty
from consul import Consul

from messaging.messaging_pb2 import (
    PostRequest,
    GetResponse,
)

from messaging.messaging_pb2_grpc import (
    BaseServicer,
    add_BaseServicer_to_server,
)

logging.basicConfig(level=logging.INFO)


class BaseService(BaseServicer, ABC):
    def __init__(self, port: str | bytes):
        super().__init__()
        self._port = port
        self._thread_pool = futures.ThreadPoolExecutor(max_workers=8)
        self._hazelcast = HazelcastClient(
            cluster_members=['127.0.0.1:5701', '127.0.0.1:5702', '127.0.0.1:5703']
        )
        self._consul = Consul()
        name = type(self).__name__
        self._consul.agent.service.register(name, name + self._port, 'localhost', int(self._port))

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
        server = grpc.server(self._thread_pool)
        add_BaseServicer_to_server(self, server)
        server.add_insecure_port('127.0.0.1:' + self._port)
        server.start()
        logging.info(f'{self.__class__.__name__} running on port {self._port}')

        def handle_sigterm(*_):
            all_rpcs_done_event = server.stop(30)
            all_rpcs_done_event.wait(30)

        signal(SIGTERM, handle_sigterm)
        server.wait_for_termination()

    def __del__(self):
        self._hazelcast.shutdown()
        self._consul.agent.service.deregister(type(self).__name__ + self._port)
