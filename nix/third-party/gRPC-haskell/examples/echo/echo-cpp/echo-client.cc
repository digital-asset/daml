#include <string>
#include <iostream>
#include <thread>

#include <grpc++/grpc++.h>

#include "echo.grpc.pb.h"

using namespace std;
using namespace echo;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class EchoClient {
public:
  EchoClient(shared_ptr<Channel> chan) : stub_(Echo::NewStub(chan)) {}

  Status DoEcho(const string& msg){
    EchoRequest req;
    req.set_message(msg);

    EchoRequest resp;

    ClientContext ctx;

    return stub_->DoEcho(&ctx, req, &resp);
  }

private:
  unique_ptr<Echo::Stub> stub_;
};

class AddClient {
public:
  AddClient(shared_ptr<Channel> chan) : stub_(Add::NewStub(chan)) {}

  AddResponse DoAdd(const uint32_t x, const uint32_t y){
    AddRequest msg;
    msg.set_addx(x);
    msg.set_addy(y);

    AddResponse resp;

    ClientContext ctx;

    stub_->DoAdd(&ctx, msg, &resp);

    return resp;
  }
private:
  unique_ptr<Add::Stub> stub_;
};

void do10k(EchoClient* client, AddClient* addClient){
  string msg("hi");

  for(int i = 0; i < 10000; i++){
    Status status = client->DoEcho(msg);
    if(!status.ok()){
      cout<<"Error: "<<status.error_code()<<endl;
    }
    AddResponse answer = addClient->DoAdd(1,2);
  }
}

int main(){

  EchoClient client(grpc::CreateChannel("localhost:50051",
                                        grpc::InsecureChannelCredentials()));
  AddClient addClient (grpc::CreateChannel("localhost:50051",
                                        grpc::InsecureChannelCredentials()));
  thread unus(do10k,&client,&addClient);
  thread duo(do10k,&client,&addClient);
  thread tres(do10k,&client,&addClient);
  thread quattuor(do10k,&client,&addClient);
  unus.join();
  duo.join();
  tres.join();
  quattuor.join();
  return 0;
}
