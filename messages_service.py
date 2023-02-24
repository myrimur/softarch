from messaging.base_service import (
    BaseService,
    GetResponse,
    PostRequest,
    Empty,
)


class MessagesService(BaseService):
    def _get(self, request: Empty) -> GetResponse:
        return GetResponse(messages='Not implemented yet')

    def _post(self, request: PostRequest) -> Empty:
        return Empty()


if __name__ == '__main__':
    service = MessagesService('[::]:50053')
    service.run()
