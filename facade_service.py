from uuid import uuid4

from base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)

from grpc import insecure_channel
from messaging_pb2_grpc import BaseStub


class FacadeService(BaseService):
    def __init__(
            self,
            addr: str | bytes,
            logging_addr: str | bytes,
            messages_addr: str | bytes
    ):
        super().__init__(addr)
        self._logging_channel = insecure_channel(logging_addr)
        self._messages_channel = insecure_channel(messages_addr)
        self._logging_service = BaseStub(self._logging_channel)
        self._messages_service = BaseStub(self._messages_channel)

    def _get(self, request: Empty) -> GetResponse:
        logging = self._logging_service.get(Empty())
        messages = self._messages_service.get(Empty())
        return GetResponse(messages=f'logging_service: {logging.messages}; messages_service: {messages.messages}')

    def _post(self, request: PostRequest) -> Empty:
        request = PostRequest(uuid=str(uuid4()), message=request.message)
        # TODO: add retrying when failed
        self._logging_service.post(request)
        self._messages_service.post(request)
        return Empty()

    def __del__(self):
        self._logging_channel.close()
        self._messages_channel.close()


if __name__ == '__main__':
    service = FacadeService('[::]:50051', '[::]:50052', '[::]:50053')
    service.run()
