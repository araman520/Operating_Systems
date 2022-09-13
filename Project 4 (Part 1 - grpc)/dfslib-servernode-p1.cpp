#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::FileFile;
using dfs_service::CommandChannel;
using dfs_service::C_S_Command;
using dfs_service::S_C_Command;
using dfs_service::NULL_Parameter;
using dfs_service::FileStat;
using dfs_service::DFSService;


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    Status StoreFile(ServerContext* context, ServerReader<FileFile>* reader, CommandChannel* file_command) override
    {
        FileFile file_ctx;
        std::ofstream FILE;

        reader->Read(&file_ctx);

        std::string filename =  file_ctx.filename();
        int filesize = file_ctx.size();
        int size = 0;

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        FILE.open(filepath);

        while (reader->Read(&file_ctx))
        {
            if (context->IsCancelled())
            {
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                FILE.close();
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            size += file_ctx.size();
            
            FILE << file_ctx.file_chunk();
        }
        FILE.close();

        std::cout << "Received " << size << " off " <<  filesize << std::endl;

        if (size != filesize)
        {
            std::cout << "Not all file received!" << std::endl;
            return Status(StatusCode::CANCELLED, "Not all file received");
        }

        return Status::OK; 
    }

    Status FetchFile(ServerContext* context, const CommandChannel* file_command, ServerWriter<FileFile>* writer) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        FileFile file_ctx;

        struct stat f;

        if(stat (filepath.c_str(), &f) != 0)
        {
            std::cout << "File not found!" << std::endl;
            file_ctx.set_status(1);
            writer->Write(file_ctx);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }

        file_ctx.set_status(0);

        int filesize = f.st_size;

        file_ctx.set_size(filesize);    

        writer->Write(file_ctx);

        std::ifstream FILE(filepath);

        int sent_bytes = 0;
        int read_bytes = 0;

        int BUFFER_SIZE = 1000;

        char buffer[BUFFER_SIZE];

        while(!FILE.eof())
        {

            if (context->IsCancelled())
            {
                FILE.close();
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            file_ctx.clear_file_chunk();

            read_bytes = std::min(filesize - sent_bytes, BUFFER_SIZE);

            FILE.read(buffer, BUFFER_SIZE);

            file_ctx.set_file_chunk(buffer, read_bytes);

            file_ctx.set_size(read_bytes);

            writer->Write(file_ctx);

            sent_bytes += read_bytes;

        }
        FILE.close();

        std::cout <<"Sent "<< sent_bytes << " off " <<  filesize <<std::endl;

        if (sent_bytes != filesize)
        {
            std::cout << "Not all file sent!" << std::endl;
            return Status(StatusCode::CANCELLED, "Not all file sent");
        }

        return Status::OK; 
    }

    Status DeleteFile(ServerContext* context, const CommandChannel* file_command, S_C_Command* server_response) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        struct stat f;

        if(stat (filepath.c_str(), &f) != 0)
        {
            std::cout << "File not found!" << std::endl;
            server_response->set_command(1);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }

        if (context->IsCancelled())
        {
            std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
        }

        if(std::remove(filepath.c_str()) != 0)
        {
            server_response->set_command(2);
            return Status(StatusCode::CANCELLED, "Could not delete file");
        }
        else
        {
            server_response->set_command(0);
            return Status::OK;
        }
        
    }

    Status ListFile(ServerContext* context, const NULL_Parameter* request, ServerWriter<CommandChannel>* writer) override
    {
        DIR *dr;
        struct dirent *en;
        struct stat result;
        //dr = opendir(".");
        dr = opendir(mount_path.c_str());
        int time;
        std::string file;
        CommandChannel file_list;
        std::string filepath;

        if(!dr)
        {
            file_list.set_status(1);
            writer->Write(file_list);
            std::cout << "Directory not found!" << std::endl;
            return Status(StatusCode::NOT_FOUND, "Directory Not Found");
        }

        while ((en = readdir(dr)) != NULL)
        {
            if (context->IsCancelled())
            {
                std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
            }

            file = en->d_name;
            filepath = WrapPath(file);

            //if(stat(en->d_name, &result)==0)
            if((stat(filepath.c_str(), &result)==0))
            {
                if(result.st_mode & S_IFREG)
                {
                    time = result.st_mtime;
                    file_list.set_status(0);
                    file_list.set_filename(file);
                    file_list.set_time(time);
                    writer->Write(file_list);
                }
            }
        }
        
        closedir(dr);
        
        return Status::OK; 

    }

    Status FileStats(ServerContext* context, const CommandChannel* file_command, FileStat* file_stats) override
    {
        std::string filename = file_command->filename();

        std::cout << "File is - " << filename << std::endl;

        std::string filepath = WrapPath(filename);

        std::cout << "File is at - " << filepath << std::endl;

        struct stat f;

        if(stat(filepath.c_str(), &f) != 0)
        {
            std::cout << "File not found!" << std::endl;
            file_stats->set_status(1);
            return Status(StatusCode::NOT_FOUND, "File Not Found");
        }

        if(context->IsCancelled())
        {
            std::cout << "Deadline exceeded OR Client Disconnected!" << std::endl;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded OR Client Disconnected");
        }

        file_stats->set_status(0);

        file_stats->set_filename(filename);
        file_stats->set_size(f.st_size);
        file_stats->set_mtime(f.st_mtime);

        return Status::OK;
        
    }


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//

