from uuid import uuid4
from threading import Thread
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

logging.basicConfig(level=logging.INFO)


class FacadeService(BaseService):
    def __init__(
            self,
            addr: str | bytes,
            logging_addrs: list[str | bytes],
            messages_addr: str | bytes
    ):
        super().__init__(addr)
        self._logging_channels = [insecure_channel(logging_addr) for logging_addr in logging_addrs]
        self._messages_channel = insecure_channel(messages_addr)
        self._logging_services = [BaseStub(logging_channel) for logging_channel in self._logging_channels]
        self._messages_service = BaseStub(self._messages_channel)
        self._round_robin_idx = -1

    def _get(self, request: Empty) -> GetResponse:
        logging_response = GetResponse()
        messages_response = GetResponse()

        logging_thread = Thread(target=self._get_from_logging, args=(logging_response,))
        messages_thread = Thread(target=self._get_from_messages, args=(messages_response,))

        logging_thread.start()
        messages_thread.start()

        logging_thread.join()
        messages_thread.join()

        return GetResponse(messages=f'logging_service: {logging_response.messages};'
                                    f' messages_service: {messages_response.messages}')

    def _get_from_logging(self, logging_response: GetResponse) -> GetResponse:
        while True:
            try:
                self._round_robin_idx = (self._round_robin_idx + 1) % len(self._logging_services)
                logging_response.messages = self._logging_services[self._round_robin_idx].get(Empty()).messages
                return
            except RpcError:
                logging.error('Failed to get from logging service, retrying...')
                time.sleep(1)

    def _get_from_messages(self, messages_response: GetResponse) -> GetResponse:
        while True:
            try:
                messages_response.messages = self._messages_service.get(Empty()).messages
                return
            except RpcError:
                logging.error('Failed to get from messages service, retrying...')
                time.sleep(1)

    def _post(self, request: PostRequest) -> Empty:
        request.uuid = str(uuid4())
        logging_thread = Thread(target=self._post_to_logging, args=(request,))
        messages_thread = Thread(target=self._post_to_messages, args=(request,))

        logging_thread.start()
        messages_thread.start()

        logging_thread.join()
        messages_thread.join()

        return Empty()

    def _post_to_logging(self, request: PostRequest) -> None:
        while True:
            try:
                self._round_robin_idx = (self._round_robin_idx + 1) % len(self._logging_services)
                self._logging_services[self._round_robin_idx].post(request)
                return
            except RpcError:
                logging.error('Failed to post to logging service, retrying...')
                time.sleep(1)

    def _post_to_messages(self, request: PostRequest) -> None:
        while True:
            try:
                self._messages_service.post(request)
                return
            except RpcError:
                logging.error('Failed to post to messages service, retrying...')
                time.sleep(1)

    def __del__(self):
        for logging_channel in self._logging_channels:
            logging_channel.close()
        self._messages_channel.close()


if __name__ == '__main__':
    service = FacadeService('[::]:50051', ['[::]:50052', '[::]:50053', '[::]:50054'], '[::]:50055')
    service.run()
