import sys
from hazelcast.client import HazelcastClient

from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)

client = HazelcastClient(
    cluster_members=['127.0.0.1:5701', '127.0.0.1:5702', '127.0.0.1:5703']
)


class LoggingService(BaseService):
    def __init__(self, addr: str | bytes, map_name: str):
        super().__init__(addr)
        self._map = client.get_map(map_name).blocking()

    def _get(self, request: Empty) -> GetResponse:
        return GetResponse(messages=', '.join(self._map.values()))

    def _post(self, request: PostRequest) -> Empty:
        self._map.put(request.uuid, request.message)
        return Empty()

    def __del__(self):
        client.shutdown()


if __name__ == '__main__':
    service = LoggingService(sys.argv[1], 'messages-map')
    service.run()
