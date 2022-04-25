#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "synchronizer.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
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

Message MakeMessage(const std::string& id, const std::string& msg) {
    Message m;
    m.set_id(id);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

std::string slave_port = "";

class Client : public IClient
{
    public:
        Client(const std::string& cip,
               const std::string& cp,
               const std::string& cid)
            :coordinator_ip(cip), coordinator_port(cp), client_id(cid)
            {}
    protected:
        virtual int connectTo();
        virtual IReply processCommand(std::string& input);
        virtual void processTimeline();
    private:
        std::string coordinator_ip;
        std::string coordinator_port;
        std::string client_id;
        // You can have an instance of the client stub
        // as a member variable.
        std::unique_ptr<SNSService::Stub> sns_stub_;
        std::unique_ptr<SNSCoordinator::Stub> coordinator_stub_;

        IReply Handle();
        IReply Login();
        IReply List();
        IReply Follow(const std::string& username2);
        IReply UnFollow(const std::string& username2);
        void thread_check_master_active();
        void Timeline(const std::string& username);


};

int main(int argc, char** argv) {

    //cip, cp, p, id, t
    std::string coordinator_ip = "127.0.0.1";
    std::string coordinator_port = "8000";
    std::string client_id = "99";
    int opt = 0;
    while ((opt = getopt(argc, argv, "c:p:i:")) != -1){
        switch(opt) {
            case 'c':
                coordinator_ip = optarg;break;
            case 'p':
                coordinator_port = optarg;break;
            case 'i':
                client_id = optarg;break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    Client myc(coordinator_ip, coordinator_port, client_id);
    // You MUST invoke "run_client" function to start business logic
    myc.run_client();

    return 0;
}

int Client::connectTo()
{
	// ------------------------------------------------------------
    // In this function, you are supposed to create a stub so that
    // you call service methods in the processCommand/porcessTimeline
    // functions. That is, the stub should be accessible when you want
    // to call any service methods in those functions.
    // I recommend you to have the stub as
    // a member variable in your own Client class.
    // Please refer to gRpc tutorial how to create a stub.
	// ------------------------------------------------------------

    //USE LOGIN INFO FROM COORDINATOR
    std::string coord_login = coordinator_ip + ":" + coordinator_port;
    coordinator_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
               grpc::CreateChannel(
                    coord_login, grpc::InsecureChannelCredentials())));

    IReply hire = Handle();
    if(!hire.grpc_status.ok()) {
        return -1;
    }
    std::string login_info = "0.0.0.0:" + hire.serverinfo;
    sns_stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
               grpc::CreateChannel(
                    login_info, grpc::InsecureChannelCredentials())));

    IReply ire = Login();
    if(!ire.grpc_status.ok()) {
        return -1;
    }

    std::thread slave_switch_thread(&Client::thread_check_master_active, this);
    slave_switch_thread.detach();
    return 1;
}

IReply Client::Handle() {
    CoordRequest request;
    request.set_id(client_id);
    request.set_requester(0);
    CoordReply reply;
    ClientContext context;

    Status status = coordinator_stub_->Handle(&context, request, &reply);

    IReply ire;
    ire.grpc_status = status;
    if (reply.msg().substr(0, 5) == "Error") {
        std::cout << reply.msg() << std::endl;
        ire.comm_status = FAILURE_INVALID;
    } else {
        ire.serverinfo = reply.msg();
        ire.comm_status = SUCCESS;
    }
    return ire;
}


IReply Client::processCommand(std::string& input)
{
	// ------------------------------------------------------------
	// GUIDE 1:
	// In this function, you are supposed to parse the given input
    // command and create your own message so that you call an 
    // appropriate service method. The input command will be one
    // of the followings:
	//
	// FOLLOW <username>
	// UNFOLLOW <username>
	// LIST
    // TIMELINE
	//
	// - JOIN/LEAVE and "<username>" are separated by one space.
	// ------------------------------------------------------------
	
    // ------------------------------------------------------------
	// GUIDE 2:
	// Then, you should create a variable of IReply structure
	// provided by the client.h and initialize it according to
	// the result. Finally you can finish this function by returning
    // the IReply.
	// ------------------------------------------------------------
    
    
	// ------------------------------------------------------------
    // HINT: How to set the IReply?
    // Suppose you have "Join" service method for JOIN command,
    // IReply can be set as follow:
    // 
    //     // some codes for creating/initializing parameters for
    //     // service method
    //     IReply ire;
    //     grpc::Status status = stub_->Join(&context, /* some parameters */);
    //     ire.grpc_status = status;
    //     if (status.ok()) {
    //         ire.comm_status = SUCCESS;
    //     } else {
    //         ire.comm_status = FAILURE_NOT_EXISTS;
    //     }
    //      
    //      return ire;
    // 
    // IMPORTANT: 
    // For the command "LIST", you should set both "all_users" and 
    // "following_users" member variable of IReply.
	// ------------------------------------------------------------
    IReply ire;
    std::size_t index = input.find_first_of(" ");
    if (index != std::string::npos) {
        std::string cmd = input.substr(0, index);


        /*
        if (input.length() == index + 1) {
            std::cout << "Invalid Input -- No Arguments Given\n";
        }
        */

        std::string argument = input.substr(index+1, (input.length()-index));

        if (cmd == "FOLLOW") {
            return Follow(argument);
        }
    } else {
        if (input == "LIST") {
            return List();
        } else if (input == "TIMELINE") {
            ire.comm_status = SUCCESS;
            return ire;
        }
    }

    ire.comm_status = FAILURE_INVALID;
    return ire;
}

void Client::processTimeline()
{
    Timeline(client_id);
	// ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // for both getting and displaying messages in timeline mode.
    // You should use them as you did in hw1.
	// ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
	// ------------------------------------------------------------
}

IReply Client::List() {
    //Data being sent to the server
    Request request;
    request.set_id(client_id);

    //Container for the data from the server
    ListReply list_reply;

    //Context for the client
    ClientContext context;

    Status status = sns_stub_->List(&context, request, &list_reply);
    IReply ire;
    ire.grpc_status = status;
    //Loop through list_reply.all_users and list_reply.following_users
    //Print out the name of each room
    if(status.ok()){
        ire.comm_status = SUCCESS;
        std::string all_users;
        std::string following_users;
        for(std::string s : list_reply.all_users()){
            ire.all_users.push_back(s);
        }
        for(std::string s : list_reply.followers()){
            ire.followers.push_back(s);
        }
    }
    return ire;
}
        
IReply Client::Follow(const std::string& id2) {
    Request request;
    request.set_id(client_id);
    request.add_arguments(id2);

    Reply reply;
    ClientContext context;
    
    Status status = sns_stub_->Follow(&context, request, &reply);
    IReply ire; ire.grpc_status = status;
    if (reply.msg() == "Follow Failed -- Invalid ID") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    } else if (reply.msg() == "Follow Failed -- Already Following User") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else if (reply.msg() == "Follow Successful") {
        ire.comm_status = SUCCESS;
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

IReply Client::Login() {
    Request request;
    request.set_id(client_id);
    Reply reply;
    ClientContext context;

    Status status = sns_stub_->Login(&context, request, &reply);
    IReply ire;
    ire.grpc_status = status;
    if (reply.msg() == "You have already joined") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else if (reply.msg() == "No slave server associated. Please connect to a different cluster."){ 
        ire.comm_status = FAILURE_INVALID;
    } else {
        slave_port = reply.msg();
        std::cout << "Connected to cluster #" << (std::stoi(client_id) % 3) + 1 << std::endl;  
        ire.comm_status = SUCCESS;
    }
    return ire;
}

void Client::thread_check_master_active(){
    while (1){
        CoordReply r;
        ClientContext context;
        CoordRequest cr;
        cr.set_requester(99);
        cr.set_port_number(slave_port);
        coordinator_stub_->Handle(&context, cr, &r);
        if (r.msg() == "false"){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));  
    }
    std::string login_info = "0.0.0.0:" + slave_port;
    sns_stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
               grpc::CreateChannel(
                    login_info, grpc::InsecureChannelCredentials())));
    std::cout << std::endl << "Redirecting to slave server at port " << slave_port << "..." << std::endl;
    IReply ire = Login();
}

void Client::Timeline(const std::string& username) {
    ClientContext context;

    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
            sns_stub_->Timeline(&context));

    //Thread used to read chat messages and send them to the server
    std::thread writer([username, stream]() {
            std::string input = "Set Stream";
            Message m = MakeMessage(username, input);
            stream->Write(m);
            while (1) {
            input = getPostMessage();
            m = MakeMessage(username, input);
            stream->Write(m);
            }
            stream->WritesDone();
            });

    std::thread reader([username, stream]() {
            Message m;
            while(stream->Read(&m)){

            google::protobuf::Timestamp temptime = m.timestamp();
            std::time_t time = temptime.seconds();
            displayPostMessage(m.id(), m.msg(), time);
            }
            });

    //Wait for the threads to finish
    writer.join();
    reader.join();
}

