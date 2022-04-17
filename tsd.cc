/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <chrono>
#include <thread>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"
#include "client.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::ClientContext;
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
using snsCoordinator::ServerType;
using snsCoordinator::RequesterType;
using snsCoordinator::CoordRequest;
using snsCoordinator::CoordReply;
using snsCoordinator::HeartBeat;
using google::protobuf::util::TimeUtil;

struct Client {
  std::string id;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (id == c1.id);
  }
};

//Vector that stores every client that has been created
std::vector<Client> client_db;

//stub to connect to the coordinator
std::unique_ptr<SNSCoordinator::Stub> coordinator_stub_;

//Helper function used to find a Client object given its ID
int find_user(std::string id){
  int index = 0;
  for(Client c : client_db){
    if(c.id == id)
      return index;
    index++;
  }
  return -1;
}

std::string Handle(std::string server_id, std::string type, std::string port) {
    CoordRequest request;
    request.set_id(server_id);
    request.set_requester(1);
    request.set_server_type(type);
    request.set_port_number(port);
    CoordReply reply;
    ClientContext context;

    Status status = coordinator_stub_->Handle(&context, request, &reply);
    return reply.msg();
}

void thread_heartbeat_func(std::string server_id, std::string server_type){
  while(1){
    HeartBeat heart;
    heart.set_sid(server_id);
    heart.set_s_type(server_type);
    Timestamp timestamp;
    timestamp.set_seconds(time(NULL));
    int64_t time_num = TimeUtil::TimestampToSeconds(timestamp);
    heart.set_timestamp(time_num);
    CoordReply reply;
    ClientContext context;

    Status status = coordinator_stub_->ServerCommunicate(&context, heart, &reply);
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    Client user = client_db[find_user(request->id())];
    int index = 0;
    for(Client c : client_db){
      list_reply->add_all_users(c.id);
    }
    std::vector<Client*>::const_iterator it;
    for(it = user.client_followers.begin(); it!=user.client_followers.end(); it++){
      list_reply->add_followers((*it)->id);
    }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string id1 = request->id();
    std::string id2 = request->arguments(0);
    int join_index = find_user(id2);
    if(join_index < 0 || id1 == id2)
      reply->set_msg("Follow Failed -- Invalid ID");
    else{
      Client *user1 = &client_db[find_user(id1)];
      Client *user2 = &client_db[join_index];
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()){
	reply->set_msg("Follow Failed -- Already Following User");
        return Status::OK;
      }
      user1->client_following.push_back(user2);
      user2->client_followers.push_back(user1);
      reply->set_msg("Follow Successful");
    }
    return Status::OK; 
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    Client c;
    std::string id = request->id();
    int user_index = find_user(id);
    if(user_index < 0){
      c.id = id;
      client_db.push_back(c);
      reply->set_msg("Login Successful!");
    }
    else{ 
      Client *user = &client_db[user_index];
      if(user->connected)
        reply->set_msg("You have already joined");
      else{
        std::string msg = "Welcome Back UID:" + user->id;
	reply->set_msg(msg);
        user->connected = true;
      }
    }
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message message;
    Client *c;
    while(stream->Read(&message)) {
      std::string id = message.id();
      int user_index = find_user(id);
      c = &client_db[user_index];
 
      //Write the current message to "id.txt"
      std::string filename = id +".txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = time+" :: "+message.id()+":"+message.msg()+"\n";
      //"Set Stream" is the default message from the client to initialize the stream
      if(message.msg() != "Set Stream")
        user_file << fileinput;
      //If message = "Set Stream", print the first 20 chats from the people you follow
      else{
        if(c->stream==0)
      	  c->stream = stream;
        std::string line;
        std::vector<std::string> newest_twenty;
        std::ifstream in(id+"following.txt");
        int count = 0;
        //Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
        while(getline(in, line)){
          if(c->following_file_size > 20){
	    if(count < c->following_file_size-20){
              count++;
	      continue;
            }
          }
          newest_twenty.push_back(line);
        }
        Message new_msg; 
 	//Send the newest messages to the client to be displayed
	for(int i = 0; i<newest_twenty.size(); i++){
	  new_msg.set_msg(newest_twenty[i]);
          stream->Write(new_msg);
        }    
        continue;
      }
      //Send the message to each follower's stream
      std::vector<Client*>::const_iterator it;
      for(it = c->client_followers.begin(); it!=c->client_followers.end(); it++){
        Client *temp_client = *it;
      	if(temp_client->stream!=0 && temp_client->connected)
	  temp_client->stream->Write(message);
        //For each of the current user's followers, put the message in their following.txt file
        std::string temp_id = temp_client->id;
        std::string temp_file = temp_id + "following.txt";
	std::ofstream following_file(temp_file,std::ios::app|std::ios::out|std::ios::in);
	following_file << fileinput;
        temp_client->following_file_size++;
	std::ofstream user_file(temp_id + ".txt",std::ios::app|std::ios::out|std::ios::in);
        user_file << fileinput;
      }
    }
    //If the client disconnected from Chat Mode, set connected to false
    c->connected = false;
    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  std::string coordinator_ip = "127.0.0.1";
  std::string coordinator_port = "8000";
  std::string id = "1";
  std::string type = "master";
  int opt = 0;
  //cip, cp, p, id, t
  while ((opt = getopt(argc, argv, "c:o:p:i:t:")) != -1){
    switch(opt) {
      case 'c':
        coordinator_ip = optarg;break;
      case 'o':
        coordinator_port = optarg;break;
      case 'p':
        port = optarg;break;
      case 'i':
        id = optarg;break;
      case 't':
        type = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  std::string coord_login = coordinator_ip + ":" + coordinator_port;
  coordinator_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
              grpc::CreateChannel(
                  coord_login, grpc::InsecureChannelCredentials())));
  //run handle service
  std::string handle_msg = Handle(id, type, port);
  if (handle_msg.substr(0, 5) == "Error"){
    std::cout << "Connection from server to coordinator failed: " + handle_msg << std::endl;
    exit(1);
  }
  //thread
  std::thread hb(thread_heartbeat_func, id, type);
  std::cout << handle_msg << std::endl;
  //if all slots are taken for table requested (master/slave), return with error
  //
  RunServer(port);
  hb.join();
  return 0;
}
