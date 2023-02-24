from google.protobuf.empty_pb2 import Empty
import grpc
from messaging.messaging_pb2_grpc import BaseStub
from messaging.messaging_pb2 import PostRequest

with grpc.insecure_channel("localhost:50051") as channel:
    service = BaseStub(channel)
    while True:
        input_ = input('> ').split(' ')
        if len(input_) == 0:
            continue
        elif len(input_) == 1 and input_[0] in {'q', 'quit', 'exit'}:
            break
        elif len(input_) == 1 and input_[0] == 'get':
            print(service.get(Empty()))
        elif len(input_) == 2 and input_[0] == 'post':
            service.post(PostRequest(message=input_[1]))
        else:
            print('Unknown command')
