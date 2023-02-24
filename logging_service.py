from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)

messages = {}


class LoggingService(BaseService):
    def _get(self, request: Empty) -> GetResponse:
        return GetResponse(messages=', '.join(messages.values()))

    def _post(self, request: PostRequest) -> Empty:
        messages[request.uuid] = request.message
        return Empty()


if __name__ == '__main__':
    service = LoggingService('[::]:50052')
    service.run()
