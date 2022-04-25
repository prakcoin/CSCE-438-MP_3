#include <iostream>
#include <fstream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>

#include <google/protobuf/util/time_util.h>
#include "synchronizer.grpc.pb.h"
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::CoordRequest;
using snsCoordinator::CoordReply;
using snsCoordinator::HeartBeat;
using snsSynchronizer::SNSSynchronizer;
using snsSynchronizer::SyncReply;
using snsSynchronizer::SyncRequest;
using google::protobuf::util::TimeUtil;

//used to send master server a notification that it has a slave
std::unique_ptr<SNSCoordinator::Stub> c_stub_;

//used to send master server a notification that it has a slave
std::unique_ptr<SNSSynchronizer::Stub> sync_stub_;

//filename
std::string filename = "follower_file";

std::string Handle(std::string sync_id, std::string port) {
    CoordRequest request;
    request.set_id(sync_id);
    request.set_requester(2);
    request.set_port_number(port);
    CoordReply reply;
    ClientContext context;

    Status status = c_stub_->Handle(&context, request, &reply);
    return reply.msg();
}

void master_thread_function(std::string id, std::string current_port){
  std::string cluster_id = std::to_string(stoi(id) % 3);
  std::string master_full_name = filename + cluster_id + ".txt";
  while (true){
    std::ifstream in(master_full_name);
    std::string line;
    if (!std::getline(in, line)){
      in.close();
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }

    int split_index = line.find_first_of("follows");
    std::string id1 = line.substr(0, split_index - 1);
    std::string id2 = line.substr(split_index + strlen("follows") + 1, line.length() - (split_index + strlen("follows")));

    CoordRequest request;
    request.set_id(id2);
    request.set_requester(50);
    CoordReply reply;
    ClientContext context;
    Status status = c_stub_->Handle(&context, request, &reply);
    std::string id2_sync_port = reply.msg();

    if (id2_sync_port.substr(0, 5) == "Error"){
      std::cout << id2_sync_port << std::endl;
      in.close();
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    } 
    
    if (current_port == id2_sync_port){
        continue;
    }
    
    std::string sync_login = "127.0.0.1:" + id2_sync_port;
    sync_stub_ = std::unique_ptr<SNSSynchronizer::Stub>(SNSSynchronizer::NewStub(
              grpc::CreateChannel(
                  sync_login, grpc::InsecureChannelCredentials())));

    ClientContext c;
    SyncRequest req;
    SyncReply rep;

    std::string id2_cluster_id = std::to_string(stoi(id2) % 3);

    req.set_id(id2_cluster_id);
    req.set_msg(line);
    sync_stub_->FollowerSynchronizer(&c, req, &rep);
    in.close();
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
}

class SNSSynchronizerImpl final : public SNSSynchronizer::Service {
  Status FollowerSynchronizer(ServerContext* context, const SyncRequest* request, SyncReply* reply) override {
    std::string msg = request->msg();
    std::string clust_id = request->id();
    std::string master_f2server_file = "fsync_to_server_file" + clust_id + ".txt";
    
    std::ofstream master_fsync_to_server_file(master_f2server_file, std::ios::app|std::ios::out|std::ios::in);
    master_fsync_to_server_file << msg << "\n";
    master_fsync_to_server_file.close();
    
    return Status::OK;
  }
};

void RunSynchronizerServer(std::string port_no) { //only to talk to other syncers
  std::string server_address = "127.0.0.1:"+port_no;
  SNSSynchronizerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Synchronizer listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  std::string coordinator_ip = "127.0.0.1";
  std::string coordinator_port = "8000";
  std::string port = "9000";
  std::string id = "1";
  int opt = 0;

  while ((opt = getopt(argc, argv, "c:o:p:i:")) != -1){
    switch(opt) {
      case 'c':
        coordinator_ip = optarg;break;
      case 'o':
        coordinator_port = optarg;break;
      case 'p':
        port = optarg;break;
      case 'i':
        id = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  std::string coord_login = coordinator_ip + ":" + coordinator_port;
  c_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
              grpc::CreateChannel(
                  coord_login, grpc::InsecureChannelCredentials())));


  
  std::string handle_msg = Handle(id, port);
  if (handle_msg.substr(0, 5) == "Error"){
    std::cout << handle_msg << std::endl;
    exit(0);
  }

  
  std::thread t1(master_thread_function, id, port);
  t1.detach();
  RunSynchronizerServer(port);
  return 0;
}