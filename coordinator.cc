#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <fstream>
#include <grpc++/grpc++.h>
#include <sys/stat.h>
#include "client.h"

#include <google/protobuf/util/time_util.h>
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
using google::protobuf::util::TimeUtil;

struct server_struct{
    std::string server_id;
    std::string port_num;
    std::string type;
    int64_t heartbeatstamp;
    bool active = false;
};

std::vector<server_struct> master_table(3);
std::vector<server_struct> slave_table(3);
std::vector<server_struct> sync_table(3);

//Hashmap for ids and ports
std::map<std::string, int> client_ids; 
std::map<std::string, bool> master_ids; 
std::map<std::string, bool> slave_ids;
std::map<std::string, bool> sync_ids;
std::map<std::string, bool> ports;

//table indices
int master_index = 0;
int slave_index = 0;
int sync_index = 0;

//used to send master server a notification that it has a slave
std::unique_ptr<SNSService::Stub> sns_stub_;

int get_table_index (std::string id, std::string type){
    if (type == "master"){
        for (int i = 0; i < master_table.size(); i++){
            if (id == master_table[i].server_id){
                return i;
            }
        }
    } else if (type == "slave"){
        for (int i = 0; i < slave_table.size(); i++){
            if (id == slave_table[i].server_id){
                return i;
            }
        }
    }
    return -1;
}

void print_client_db(){
    std::map<std::string, int>::iterator it = client_ids.begin();
    while (it != client_ids.end()){
        std::cout << it->first << " " << it->second << std::endl;
        ++it;
    }
}

void thread_function(int index, std::string type){
    if (type == "master"){
        while(TimeUtil::TimestampToSeconds(TimeUtil::TimeTToTimestamp(time(NULL))) - master_table[index].heartbeatstamp <= 11){
            //std::cout << "master wait " << TimeUtil::TimestampToSeconds(TimeUtil::TimeTToTimestamp(time(NULL))) - master_table[index].heartbeatstamp << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        server_struct s;
        master_table[index] = s;
        std::cout << "Master server closed!" << std::endl;
        //transfer data to slave
        

    } else if (type == "slave"){
        while(TimeUtil::TimestampToSeconds(TimeUtil::TimeTToTimestamp(time(NULL))) - slave_table[index].heartbeatstamp <= 11){
            //std::cout << "slave wait " << TimeUtil::TimestampToSeconds(TimeUtil::TimeTToTimestamp(time(NULL))) - master_table[index].heartbeatstamp << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        server_struct s;
        slave_table[index] = s;
        std::cout << "Slave server closed!" << std::endl;
    }
}

std::string get_server_port(int client_id){ //MAKE CID INT SINCE WE ARE COMPUTING MOD
    int server_id = (client_id % 3);
    if (master_table[server_id].active == true){
        return master_table[server_id].port_num;
    } else if (slave_table[server_id].active == true){
        return slave_table[server_id].port_num;
    }
    return "";
}

std::string get_sync_port(int client_id){ 
    int sync_id = (client_id % 3);
    if (sync_table[sync_id].active == true){
        return sync_table[sync_id].port_num;
    }
    return "";
}

void send_slave_to_master(int index){
    std::string port = master_table[index].port_num;
    std::string login_info = "0.0.0.0:" + port;
    sns_stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
               grpc::CreateChannel(
                    login_info, grpc::InsecureChannelCredentials())));
    ClientContext c;
    Reply r;
    Message m;
    m.set_msg(slave_table[index].port_num);
    m.set_id(slave_table[index].server_id);
    sns_stub_->RecSlave(&c, m, &r);
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
                client_ids[client_id] = stoi(client_id) % 3;
                if (!server_port.empty()){
                    reply->set_msg(server_port);
                } else {
                    reply->set_msg("Error: No servers active");
                }
            } else {
                reply->set_msg("Error: Client ID taken.");
            }
            //print_client_db(); //PRINTING THE CLIENTS
        } else if (requester == 1) { //server
            std::string port_num = request->port_number();
            std::string server_id = request->id();
            std::string server_type = request->server_type();
            if (ports.find(port_num) != ports.end()){
                 reply->set_msg("Error: Server Port taken.");
                 return Status::OK;
            }
            ports[port_num] = true;
            if (server_type == "master"){
                if (master_index <= 2){
                    if (master_ids.find(server_id) == master_ids.end()){
                        master_ids[server_id] = true;
                        master_table[master_index].server_id = server_id;
                        master_table[master_index].port_num = port_num;
                        master_table[master_index].type = server_type;
                        reply->set_msg("Master server ID:" + server_id + " connected to cluster #" + std::to_string(master_index + 1));
                        master_index++;
                    } else {
                        reply->set_msg("Error: Server ID taken.");
                    }
                } else {
                    reply->set_msg("Error: Clusters full.");
                }               
            } else if (server_type == "slave"){
                if (slave_index <= 2){
                    if (slave_ids.find(server_id) == slave_ids.end()){
                        slave_ids[server_id] = true;
                        slave_table[slave_index].server_id = server_id;
                        slave_table[slave_index].port_num = port_num;
                        slave_table[slave_index].type = server_type;
                        //notify master server
                        if (master_table[slave_index].active == true){
                            send_slave_to_master(slave_index);
                        }
                        reply->set_msg("Slave server ID:" + server_id + " connected to cluster #" + std::to_string(slave_index + 1));
                        slave_index++;
                    } else {
                        reply->set_msg("Error: Server ID taken.");
                    }
                } else {
                    reply->set_msg("Error: Clusters full.");
                }        
            }

        } else if (requester == 2) { //synchronizer
            if (sync_index <= 2){
                std::string sync_id = request->id();
                std::string sync_port = request->port_number();
                if (ports.find(sync_port) != ports.end()){ //check if id is taken
                    reply->set_msg("Error: Port taken.");
                    return Status::OK;
                } 
                ports[sync_port] = true;
                if (sync_ids.find(sync_id) != sync_ids.end()){ //check if id is taken
                    reply->set_msg("Error: Sync ID taken.");
                    return Status::OK;
                }
                sync_ids[sync_id] = true; 
                sync_table[sync_index].server_id = sync_id;
                sync_table[sync_index].port_num = sync_port;
                sync_table[sync_index].active = true;
                reply->set_msg("Sync ID:" + sync_id + " connected to cluster #" + std::to_string(sync_index + 1));
                sync_index++;
            } else {
                reply->set_msg("Error: Clusters full.");
            }    
        } else if (requester == 50){
            std::string cli_id = request->id();
            std::string sync_port = get_sync_port(stoi(cli_id));
            if (!sync_port.empty()){
                reply->set_msg(sync_port);
            } else {
                reply->set_msg("Error: Sync port not found.");
            }
        } else if (requester == 51){
            std::string cli_id = request->id();
            if (client_ids.find(cli_id) != client_ids.end()){
                reply->set_msg(std::to_string(client_ids[cli_id]));
            } else {
                reply->set_msg("Error: No such user.");
            }
        } else if (requester == 99){
            std::string slave_port = request->port_number();
            for (int i = 0; i < slave_table.size(); i++){
                if (slave_table[i].port_num == slave_port){        
                    if (master_table[i].active == false){
                        reply->set_msg("false");
                    } else {
                        reply->set_msg("true");
                    }
                }
            }
        }
        return Status::OK;
    }

    //heartbeat
    Status ServerCommunicate(ServerContext* context, const HeartBeat* heart, CoordReply* reply){
        google::protobuf::Timestamp t = TimeUtil::SecondsToTimestamp(heart->timestamp());
        std::string server_id = heart->sid();
        std::string server_type = heart->s_type();
        int64_t timestamp_int = TimeUtil::TimestampToSeconds(t);
        int table_index = get_table_index(server_id, server_type);

        if (server_type == "master"){ 
            if (master_table[table_index].heartbeatstamp == 0 && master_table[table_index].active == false){
                master_table[table_index].active = true; 
                //start a thread
                std::thread t(thread_function, table_index, server_type);
                t.detach();
            }
            master_table[table_index].heartbeatstamp = timestamp_int;
        } else if (server_type == "slave"){
            if (slave_table[table_index].heartbeatstamp == 0 && slave_table[table_index].active == false){
                slave_table[table_index].active = true; 
                //start a thread
                std::thread t(thread_function, table_index, server_type);
                t.detach();
            }
            slave_table[table_index].heartbeatstamp = timestamp_int;
        }

        std::cout << "Server Type:'" << server_type << "' ID#" << server_id << " sent heartbeat: " << /*TimeUtil::ToString(t)*/ timestamp_int << std::endl; // debug
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

    std::string f2server_file = "fsync_to_server_file0.txt";
    std::ofstream fsync_to_server_file1(f2server_file, std::ios::trunc|std::ios::out|std::ios::in);
    fsync_to_server_file1.close();
    f2server_file = "fsync_to_server_file1.txt";
    std::ofstream fsync_to_server_file2(f2server_file, std::ios::trunc|std::ios::out|std::ios::in);
    fsync_to_server_file2.close();
    f2server_file = "fsync_to_server_file2.txt";
    std::ofstream fsync_to_server_file3(f2server_file, std::ios::trunc|std::ios::out|std::ios::in);
    fsync_to_server_file3.close();

    mkdir("master", 0777);
    mkdir("slave", 0777);
    RunCoordServer(port);
    return 0;
}