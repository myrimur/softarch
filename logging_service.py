import sys

from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)


class LoggingService(BaseService):
    def __init__(self, addr: str | bytes):
        super().__init__(addr)
        self._map = self._client.get_map('messages-map').blocking()

    def _get(self, request: Empty) -> GetResponse:
        return GetResponse(messages=', '.join(self._map.values()))

    def _post(self, request: PostRequest) -> Empty:
        self._map.put(request.uuid, request.message)
        return Empty()


if __name__ == '__main__':
    LoggingService(sys.argv[1]).run()
