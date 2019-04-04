#include <string>
#include <iostream>
#include <atomic>

#include <grpc++/grpc++.h>
#include "echo.grpc.pb.h"

using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace echo;

atomic_int reqCount;

class EchoServiceImpl final : public Echo::Service {
    Status DoEcho(ServerContext* ctx, const EchoRequest* req,
                  EchoRequest* resp) override {
      resp->set_message(req->message());
      return Status::OK;
    }
};

class AddServiceImpl final : public Add::Service {
    Status DoAdd(ServerContext* ctx, const AddRequest* req,
                 AddResponse* resp) override {
      resp->set_answer(req->addx() + req->addy());
      return Status::OK;
    }
};

int main(){
  string server_address("localhost:50051");
  EchoServiceImpl echoService;
  AddServiceImpl addService;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&echoService);
  builder.RegisterService(&addService);
  unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
  return 0;
}
