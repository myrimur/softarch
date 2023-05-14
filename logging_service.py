import sys

from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)


class LoggingService(BaseService):
    def __init__(self, port: str | bytes):
        super().__init__(port)

        self._map = self._hazelcast.get_map(
            self._consul.kv.get('map-name')[1]['Value'].decode('utf-8')
        ).blocking()

    def _get(self, request: Empty) -> GetResponse:
        return GetResponse(messages=', '.join(self._map.values()))

    def _post(self, request: PostRequest) -> Empty:
        self._map.put(request.uuid, request.message)
        return Empty()


if __name__ == '__main__':
    LoggingService(sys.argv[1]).run()
