#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"

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
using snsCoordinator::SNSCoordinator;
using snsCoordinator::ServerType;
using snsCoordinator::RequesterType;
using snsCoordinator::CoordRequest;
using snsCoordinator::CoordReply;
using snsCoordinator::HeartBeat;

struct server_struct{
    std::string server_id;
    std::string port_num;
    std::string type;
    bool active = false;
};

std::vector<server_struct> master_table(3);
std::vector<server_struct> slave_table(3);
std::vector<server_struct> synchronizer_table(3);

//Hashmap for ids and ports
std::map<std::string, bool> client_ids;
std::map<std::string, bool> server_ids;
std::map<std::string, bool> ports;

int master_index = 0;
int slave_index = 0;

std::string get_server_port(int client_id){ //MAKE CID INT SINCE WE ARE COMPUTING MOD
    int server_id = (client_id % 3) + 1;
    if (master_table[server_id].active == true){
        return master_table[server_id].port_num;
    } else if (slave_table[server_id].active == true){
        return slave_table[server_id].port_num;
    }
    return "";
}

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
    //manage client req
    //manage master/slave req

    Status Handle(ServerContext* context, const CoordRequest* request, CoordReply* reply) override {
        int requester = request->requester();
        if (requester == 0){ //client
            std::string client_id = request->id();
            std::string server_port = get_server_port(stoi(client_id));
            if (client_ids.find(client_id) == client_ids.end()){ //check if client id is taken
                client_ids[client_id] = true;
                if (!server_port.empty()){
                    reply->set_msg(server_port);
                } else {
                    reply->set_msg("Error: No servers active");
                }
            } else {
                reply->set_msg("Error: Client ID taken.");
            }
        } else if (requester == 1) { //server
            std::string port_num = request->port_number();
            std::string server_id = request->id();
            std::string server_type = request->server_type();
            if (ports.find(port_num) != ports.end()){
                 reply->set_msg("Error: Server Port taken.");
            }
            ports[port_num] = true;
            if (server_type == "master"){
                if (master_index <= 2){
                    if (server_ids.find(server_id) == server_ids.end()){
                        server_ids[server_id] = true;
                        master_table[master_index].server_id = server_id;
                        master_table[master_index].port_num = port_num;
                        master_table[master_index].type = server_type;
                        master_table[master_index].active = true;
                        reply->set_msg("Master server ID:" + server_id + " connected to cluster #" + std::to_string(master_index));
                        master_index++;
                    } else {
                        reply->set_msg("Error: Server ID taken.");
                    }
                } else {
                    reply->set_msg("Error: Clusters full.");
                }               
            } else if (server_type == "slave"){
                if (slave_index <= 2){
                    if (server_ids.find(server_id) == server_ids.end()){
                        server_ids[server_id] = true;
                        slave_table[slave_index].server_id = server_id;
                        slave_table[slave_index].port_num = port_num;
                        slave_table[slave_index].type = server_type;
                        slave_table[slave_index].active = true;
                        reply->set_msg("Slave server ID:" + server_id + " connected to cluster #" + std::to_string(slave_index));
                        slave_index++;
                    } else {
                        reply->set_msg("Error: Server ID taken.");
                    }
                } else {
                    reply->set_msg("Error: Clusters full.");
                }        
            }

        } else if (requester == 2) { //synchronizer

        }
        return Status::OK;
    }
};

void RunCoordServer(std::string port_no) {
  std::string server_address = "127.0.0.1:"+port_no;
  SNSCoordinatorImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Coordinator listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "8000";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunCoordServer(port);
  return 0;
}