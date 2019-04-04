from grpc.beta import implementations

import echo_pb2

def main():
    channel = implementations.insecure_channel('localhost', 50051)
    stub = echo_pb2.beta_create_Echo_stub(channel)
    message = echo_pb2.EchoRequest(message='foo')
    response = stub.DoEcho(message, 15)

if __name__ == '__main__':
    main()
