from threading import Thread, Lock
import logging
import sys

from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)


class MessagesService(BaseService):
    def __init__(self, port: str | bytes):
        super().__init__(port)
        self._queue = self._hazelcast.get_queue(
            self._consul.kv.get('queue-name')[1]['Value'].decode('utf-8')
        ).blocking()
        self._messages = []
        self._lock = Lock()

    def _consume(self):
        while True:
            msg = self._queue.take()
            with self._lock:
                self._messages.append(msg)
            logging.info(f'{self.__class__.__name__}.consume: {msg}')

    def _get(self, request: Empty) -> GetResponse:
        with self._lock:
            return GetResponse(messages=', '.join(self._messages))

    def _post(self, request: PostRequest) -> Empty:
        return Empty()

    def run(self):
        Thread(target=self._consume, daemon=True).start()
        super().run()


if __name__ == '__main__':
    MessagesService(sys.argv[1]).run()
