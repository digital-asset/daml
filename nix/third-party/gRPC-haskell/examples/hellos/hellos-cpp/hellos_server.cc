#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "hellos.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::Status;
using hellos::SSRqt;
using hellos::SSRpy;
using hellos::CSRqt;
using hellos::CSRpy;
using hellos::BiRqtRpy;
using hellos::Hellos;

static void Die(const std::string& msg) {
  std::cerr << "Fatal error: " << msg << std::endl;
  exit(1);
}

class HellosImpl final : public Hellos::Service {
  Status HelloSS(ServerContext* context,
                 const SSRqt* rqt,
                 ServerWriter<SSRpy>* writer) override {
    for (unsigned i = 0; i < rqt->num_replies(); ++i) {
      SSRpy rpy;
      rpy.set_greeting("Hello there, " + rqt->name() + "!");
      writer->Write(rpy);
    }
    return Status::OK;
  }

  Status HelloCS(ServerContext* context,
                 ServerReader<CSRqt>* reader,
                 CSRpy* rpy) override {
    CSRqt rqt;
    unsigned rqtCnt = 0;
    std::string ex("client streaming payload");
    while (reader->Read(&rqt)) {
      if (rqt.message() != ex)
        Die("HelloCS/rpy: expected payload '" + ex +
            "', got '" + rqt.message() + "'");
      ++rqtCnt;
    }
    rpy->set_num_requests(rqtCnt);
    return Status::OK;
  }

  Status HelloBi(ServerContext* context,
                 ServerReaderWriter<BiRqtRpy, BiRqtRpy>* strm) override {
    BiRqtRpy rqt;
    strm->SendInitialMetadata();
    while (strm->Read(&rqt)) {
      strm->Write(rqt);
    }
    return Status::OK;
  }

};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  HellosImpl service;
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();
  return 0;
}
