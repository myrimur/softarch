import sys
from uuid import uuid4
from concurrent.futures import wait
from random import choice
import logging
import time

from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)

from grpc import insecure_channel, RpcError
from messaging.messaging_pb2_grpc import BaseStub


class FacadeService(BaseService):
    def __init__(self, port: str | bytes):
        super().__init__(port)
        self._logging_channels = [
            insecure_channel(serv['Address'] + ':' + str(serv['ServicePort'])) for serv in
            self._consul.catalog.service('LoggingService')[1]
        ]
        self._messages_channels = [
            insecure_channel(serv['Address'] + ':' + str(serv['ServicePort'])) for serv in
            self._consul.catalog.service('MessagesService')[1]
        ]
        self._logging_services = [BaseStub(chan) for chan in self._logging_channels]
        self._messages_services = [BaseStub(chan) for chan in self._messages_channels]
        self._queue = self._hazelcast.get_queue(
            self._consul.kv.get('queue-name')[1]['Value'].decode('utf-8')
        )

    def _get(self, request: Empty) -> GetResponse:
        logging_response = GetResponse()
        messages_response = GetResponse()

        wait([self._thread_pool.submit(self._get_from_logging, logging_response),
              self._thread_pool.submit(self._get_from_messages, messages_response)])

        return GetResponse(messages=f'logging_service: {logging_response.messages};'
                                    f' messages_service: {messages_response.messages}')

    def _get_from_logging(self, logging_response: GetResponse) -> GetResponse:
        while True:
            try:
                logging_response.messages = choice(self._logging_services).get(Empty()).messages
                return
            except RpcError:
                logging.error('Failed to get from logging service, retrying...')
                time.sleep(1)

    def _get_from_messages(self, messages_response: GetResponse) -> GetResponse:
        while True:
            try:
                messages_response.messages = choice(self._messages_services).get(Empty()).messages
                return
            except RpcError:
                logging.error('Failed to get from messages service, retrying...')
                time.sleep(1)

    def _post(self, request: PostRequest) -> Empty:
        request.uuid = str(uuid4())

        future = self._queue.put(request.message)
        self._post_to_logging(request)
        future.result()

        return Empty()

    def _post_to_logging(self, request: PostRequest) -> None:
        while True:
            try:
                choice(self._logging_services).post(request)
                return
            except RpcError:
                logging.error('Failed to post to logging service, retrying...')
                time.sleep(1)

    def __del__(self):
        for channel in self._logging_channels + self._messages_channels:
            channel.close()


if __name__ == '__main__':
    FacadeService(sys.argv[1]).run()
