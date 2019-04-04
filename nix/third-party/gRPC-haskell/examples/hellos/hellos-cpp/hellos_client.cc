#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>

#include "hellos.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::ClientReaderWriter;
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

static void CheckRPCStatus(const std::string& ctx, Status status) {
  if (status.ok()) {
    std::cout << ctx << ": RPC successful." << std::endl;
  } else {
    std::cout << ctx << ": Got failed status code: " << status.error_code()    << std::endl;
    std::cout << ctx << ": Got failed status msg: "  << status.error_message() << std::endl;
    Die(ctx + ": RPC failed");
  }
}

class HellosClient {
 public:
  HellosClient(std::shared_ptr<Channel> channel)
      : stub_(Hellos::NewStub(channel)) {}

  void DoHelloSS(const std::string& name, unsigned n) {
    SSRqt rqt;
    rqt.set_name(name);
    rqt.set_num_replies(n);

    SSRpy rpy;
    ClientContext ctx;

    std::unique_ptr<ClientReader<SSRpy> > reader(stub_->HelloSS(&ctx, rqt));
    unsigned rpyCnt = 0;
    while (reader->Read(&rpy)) {
      ++rpyCnt;
      std::string ex("Hello there, " + name + "!");
      if (rpy.greeting() != ex) {
        Die("DoHelloSS/rpy: expected payload '" + ex +
              "', got '" + rpy.greeting() + "'");
      }
    }
    Status status = reader->Finish();
    CheckRPCStatus("DoHelloSS", status);
    if (rpyCnt != n)
      Die("DoHelloSS/cnt: expected " + std::to_string(n) +
            "replies, got " + std::to_string(rpyCnt));
  }

  void DoHelloCS(const std::string& pay, unsigned n) {
    CSRqt rqt;
    rqt.set_message(pay);

    CSRpy rpy;
    ClientContext ctx;

    std::unique_ptr<ClientWriter<CSRqt> > writer(stub_->HelloCS(&ctx, &rpy));
    for (unsigned i = 0; i < n; ++i) {
      if (!writer->Write(rqt)) {
        // Broken stream
        break;
      }
    }
    writer->WritesDone();
    Status status = writer->Finish();
    CheckRPCStatus("DoHelloCS", status);
    if (rpy.num_requests() != n)
      Die("DoHelloCS/cnt: expected request count " + std::to_string(n) +
            ", got " + std::to_string(rpy.num_requests()));
  }

  void DoHelloBi(const std::string& pay, unsigned n) {
    BiRqtRpy rqt;
    rqt.set_message(pay);
    ClientContext ctx;

    std::shared_ptr<ClientReaderWriter<BiRqtRpy, BiRqtRpy> > strm(stub_->HelloBi(&ctx));

    // strm->WaitForInitialMetadata();

    // Spawn a writer thread which sends rqt to the server n times.
    std::thread writer([strm,rqt,n]() {
      for(unsigned i = 0; i < n; ++i) {
        strm->Write(rqt);
      }
      strm->WritesDone();
    });

    // Concurrently, read back echo'd replies from the server until there are no
    // more to consume; ensure we get the expected number of responses after
    // there's nothing left to read.
    BiRqtRpy rpy;
    unsigned rpyCnt = 0;

    while(strm->Read(&rpy)) {
      if (rpy.message() != pay)
        Die("DoHelloBi/rpy: expected payload '" + pay +
              "', got '" + rpy.message() + "'");
      ++rpyCnt;
    }
    writer.join();

    Status status = strm->Finish();
    CheckRPCStatus("DoHelloBi", status);
    if (rpyCnt != n)
      Die("DoHelloBi/cnt: expected reply count " + std::to_string(n) +
            ", got " + std::to_string(rpyCnt));
  }

 private:
  std::unique_ptr<Hellos::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  HellosClient hellos(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  const unsigned n = 100000;
  std::cout << "-------------- HelloSS --------------" << std::endl;
  hellos.DoHelloSS("server streaming mode", n);
  std::cout << "-------------- HelloCS --------------" << std::endl;
  hellos.DoHelloCS("client streaming payload", n);
  std::cout << "-------------- HelloBi --------------" << std::endl;
  hellos.DoHelloBi("bidi payload", n);
  return 0;
}
