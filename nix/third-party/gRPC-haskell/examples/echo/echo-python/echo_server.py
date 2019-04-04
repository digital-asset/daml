from grpc.beta import implementations

import echo_pb2
import time

class Echo(echo_pb2.BetaEchoServicer):
    def DoEcho(self, request, context):
        return echo_pb2.EchoRequest(message=request.message)

def main():
    server = echo_pb2.beta_create_Echo_server(Echo())
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        time.sleep(600)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    main()
